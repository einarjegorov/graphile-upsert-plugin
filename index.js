const debugFactory = require('debug')

const debug = debugFactory('graphile-upsert')

function PgMutationUpsertPlugin(builder) {
  builder.hook('GraphQLObjectType:fields', (fields, build, context) => {
    const {
      extend,
      newWithHooks,
      parseResolveInfo,
      pgIntrospectionResultsByKind,
      pgGetGqlTypeByTypeIdAndModifier,
      pgGetGqlInputTypeByTypeIdAndModifier,
      pgSql: sql,
      gql2pg,
      graphql: {
        GraphQLObjectType,
        GraphQLInputObjectType,
        GraphQLNonNull,
        GraphQLString,
      },
      pgColumnFilter,
      inflection,
      pgQueryFromResolveData: queryFromResolveData,
      pgOmit: omit,
      pgViaTemporaryTable: viaTemporaryTable,
      describePgEntity,
      sqlCommentByAddingTags,
      pgField,
    } = build
    const {
      scope: { isRootMutation },
      fieldWithHooks,
    } = context
    if (!isRootMutation) {
      return fields
    }

    return extend(
      fields,
      pgIntrospectionResultsByKind.class
        .filter(table => !!table.namespace)
        .filter(table => table.isSelectable)
        .filter(table => table.isInsertable && !omit(table, 'create')) // we ignore update omits for simplicity
        .reduce((memo, table) => {
          const Table = pgGetGqlTypeByTypeIdAndModifier(table.type.id, null)
          if (!Table) {
            debug(
              `There was no table type for table '${table.namespace.name}.${
                table.name
              }', so we're not generating an upsert mutation for it.`,
            )
            return memo
          }
          const TableInput = pgGetGqlInputTypeByTypeIdAndModifier(
            table.type.id,
            null,
          )
          if (!TableInput) {
            debug(
              `There was no input type for table '${table.namespace.name}.${
                table.name
              }', so we're going to omit it from the upsert mutation.`,
            )
            return memo
          }
          const tableTypeName = inflection.tableType(table)
          const InputType = newWithHooks(
            GraphQLInputObjectType,
            {
              name: `Upsert${tableTypeName}Input`,
              description: `All input for the upsert \`${tableTypeName}\` mutation.`,
              fields: {
                clientMutationId: {
                  description:
                    'An arbitrary string value with no semantic meaning. Will be included in the payload verbatim. May be used to track mutations by the client.',
                  type: GraphQLString,
                },
                ...(TableInput
                  ? {
                      [inflection.tableFieldName(table)]: {
                        description: `The \`${tableTypeName}\` to be upserted by this mutation.`,
                        type: new GraphQLNonNull(TableInput),
                      },
                    }
                  : null),
              },
            },
            {
              __origin: `Adding table upsert input type for ${describePgEntity(
                table,
              )}.`,
              // isPgCreateInputType: true,
              pgInflection: table,
            },
          )
          const PayloadType = newWithHooks(
            GraphQLObjectType,
            {
              name: `Upsert${tableTypeName}Payload`,
              description: `The output of our upsert \`${tableTypeName}\` mutation.`,
              fields: ({ fieldWithHooks }) => {
                const tableName = inflection.tableFieldName(table)
                return {
                  clientMutationId: {
                    description:
                      'The exact same `clientMutationId` that was provided in the mutation input, unchanged and unused. May be used by a client to track mutations.',
                    type: GraphQLString,
                  },
                  [tableName]: pgField(
                    build,
                    fieldWithHooks,
                    tableName,
                    {
                      description: `The \`${tableTypeName}\` that were upserted by this mutation.`,
                      type: Table,
                    },
                    {
                      // isPgCreatePayloadResultField: true,
                      pgFieldIntrospection: table,
                    },
                  ),
                }
              },
            },
            {
              __origin: `Adding table upsert payload type for ${describePgEntity(
                table,
              )}. You can  disable the built-in create mutation via:\n\n  ${sqlCommentByAddingTags(
                table,
                { omit: 'create' },
              )}`,
              isMutationPayload: true,
              // isPgCreatePayloadType: true,
              pgIntrospection: table,
            },
          )

          // Store attributes (columns) for easy access
          const attributes = pgIntrospectionResultsByKind.attribute
            .filter(attr => attr.classId === table.id)
            .filter(attr => pgColumnFilter(attr, build, context))
            .filter(attr => !omit(attr, 'create'))

          // Figure out the pkey constraint
          const primaryKeyConstraint = pgIntrospectionResultsByKind.constraint
            .filter(con => con.classId === table.id)
            .filter(con => con.type === 'p')[0]

          // Figure out to which column that pkey constraint belongs to
          const primaryKeys =
            primaryKeyConstraint &&
            primaryKeyConstraint.keyAttributeNums.map(
              num => attributes.filter(attr => attr.num === num)[0],
            )

          // Create upsert fields from each introspected table
          const fieldName = `upsert${tableTypeName}`
          memo = build.extend(
            memo,
            {
              [fieldName]: fieldWithHooks(
                fieldName,
                context => {
                  const { getDataFromParsedResolveInfoFragment } = context
                  return {
                    description: `Upserts a single \`${tableTypeName}\`.`,
                    type: PayloadType,
                    args: {
                      input: {
                        type: new GraphQLNonNull(InputType),
                      },
                    },
                    async resolve(data, { input }, { pgClient }, resolveInfo) {
                      const parsedResolveInfoFragment = parseResolveInfo(
                        resolveInfo,
                      )
                      const resolveData = getDataFromParsedResolveInfoFragment(
                        parsedResolveInfoFragment,
                        PayloadType,
                      )
                      const upsertedRowAlias = sql.identifier(Symbol())
                      const query = queryFromResolveData(
                        upsertedRowAlias,
                        upsertedRowAlias,
                        resolveData,
                        {},
                      )
                      const sqlColumns = []
                      const sqlValues = []
                      const inputData = input[inflection.tableFieldName(table)]

                      // Loop thru columns and "SQLify" them
                      attributes.forEach(attr => {
                        const key = inflection.column(attr)
                        const val = inputData[key]
                        if (
                          Object.prototype.hasOwnProperty.call(inputData, key)
                        ) {
                          sqlColumns.push(sql.identifier(attr.name))
                          sqlValues.push(
                            gql2pg(val, attr.type, attr.typeModifier),
                          )
                        }
                      })
                      const _primaryKeys = primaryKeys.map(
                        key => sql.identifier(key.name).names[0],
                      )
                      const isPrimary = i => _primaryKeys.includes(i.names[0])

                      const join = fields => sql.join(fields, ', ')

                      // Construct a array in case we need to do an update on conflict
                      const conflictUpdateArray = sqlColumns
                        .map(col => {
                          const columnName = sql.identifier(col.names[0])
                          // TODO add soft vs hard options
                          // coalesce(excluded.${columnName}, ${tableName}.${columnName})
                          return isPrimary(columnName)
                            ? null
                            : sql.query`${columnName} = excluded.${columnName}`
                        })
                        .filter(_ => _)

                      const sqlTableName = sql.identifier(
                        table.namespace.name,
                        table.name,
                      )

                      const sqlPrimaryKeys = primaryKeys.map(key =>
                        sql.identifier(key.name),
                      )

                      // SQL query for upsert mutations
                      const mutationQuery = sql.query`
                        INSERT INTO ${sqlTableName} ${
                        sqlColumns.length
                          ? sql.fragment`(
                            ${join(sqlColumns)}
                          ) VALUES (${join(sqlValues)})
                          ON CONFLICT (${join(sqlPrimaryKeys)}) DO UPDATE
                          SET ${join(conflictUpdateArray)}`
                          : sql.fragment`DEFAULT VALUES`
                      } RETURNING *`

                      let row
                      try {
                        await pgClient.query('SAVEPOINT graphql_mutation')
                        const rows = await viaTemporaryTable(
                          pgClient,
                          sql.identifier(table.namespace.name, table.name),
                          mutationQuery,
                          upsertedRowAlias,
                          query,
                        )
                        row = rows[0]
                        await pgClient.query(
                          'RELEASE SAVEPOINT graphql_mutation',
                        )
                      } catch (e) {
                        await pgClient.query(
                          'ROLLBACK TO SAVEPOINT graphql_mutation',
                        )
                        throw e
                      }
                      return {
                        clientMutationId: input.clientMutationId,
                        data: row,
                      }
                    },
                  }
                },
                {
                  pgFieldIntrospection: table,
                  // isPgCreateMutationField: true,
                },
              ),
            },
            `Adding upsert mutation for ${describePgEntity(
              table,
            )}. You can omit this mutation with:\n\n  ${sqlCommentByAddingTags(
              table,
              {
                omit: 'create',
              },
            )}`,
          )
          return memo
        }, {}),
      `Adding 'upsert' mutation to root mutation`,
    )
  })
}

module.exports = PgMutationUpsertPlugin
