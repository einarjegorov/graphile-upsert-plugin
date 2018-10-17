const debugFactory = require('debug')

const debug = debugFactory('graphile-upsert-batch')

function PgMutationUpsertBatchPlugin(builder) {
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
        GraphQLList,
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
              }', so we're not generating an upsert batch mutation for it.`,
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
              }', so we're going to omit it from the upsert batch mutation.`,
            )
            return memo
          }
          const tableTypeName = inflection.tableType(table)
          const inputFieldName = inflection.pluralize(
            inflection.tableFieldName(table),
          )
          const InputType = newWithHooks(
            GraphQLInputObjectType,
            {
              name: `Upsert${tableTypeName}BatchInput`,
              description: `All input for the upsert \`${tableTypeName}\` batch mutation.`,
              fields: {
                clientMutationId: {
                  description:
                    'An arbitrary string value with no semantic meaning. Will be included in the payload verbatim. May be used to track mutations by the client.',
                  type: GraphQLString,
                },
                ...(TableInput
                  ? {
                      [inputFieldName]: {
                        description: `The \`${inflection.pluralize(
                          tableTypeName,
                        )}\` to be upserted by this mutation. Expects all records to conform to the structure of the first.`,
                        type: new GraphQLNonNull(
                          new GraphQLList(new GraphQLNonNull(TableInput)),
                        ),
                      },
                    }
                  : null),
              },
            },
            {
              __origin: `Adding table upsert batch input type for ${describePgEntity(
                table,
              )}.`,
              // isPgCreateInputType: true,
              pgInflection: table,
            },
          )
          const PayloadType = newWithHooks(
            GraphQLObjectType,
            {
              name: `Upsert${tableTypeName}BatchPayload`,
              description: `The output of our upsert \`${tableTypeName}\` batch mutation.`,
              fields: ({ fieldWithHooks }) => {
                return {
                  clientMutationId: {
                    description:
                      'The exact same `clientMutationId` that was provided in the mutation input, unchanged and unused. May be used by a client to track mutations.',
                    type: GraphQLString,
                  },
                  [inputFieldName]: pgField(
                    build,
                    fieldWithHooks,
                    inputFieldName,
                    {
                      description: `The \`${inflection.pluralize(
                        tableTypeName,
                      )}\` that was upserted by this mutation.`,
                      type: new GraphQLList(Table),
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
              __origin: `Adding table upsert batch payload type for ${describePgEntity(
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
          const fieldName = `upsert${tableTypeName}Batch`
          memo = build.extend(
            memo,
            {
              [fieldName]: fieldWithHooks(
                fieldName,
                context => {
                  const { getDataFromParsedResolveInfoFragment } = context
                  return {
                    description: `Upserts a batch of \`${inflection.pluralize(
                      tableTypeName,
                    )}\`.`,
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
                      const upsertedRowsAlias = sql.identifier(Symbol())
                      const query = queryFromResolveData(
                        upsertedRowsAlias,
                        upsertedRowsAlias,
                        resolveData,
                        {},
                      )
                      const inputData = input[inputFieldName]

                      // no inputs, no upserts
                      if (!inputData || !inputData.length) {
                        return {
                          clientMutationId: input.clientMutationId,
                          data: [],
                        }
                      }

                      // A batch upsert must have all records conforming to the first
                      const _spec = input[inputFieldName][0]
                      const specifiedAttributes = attributes.filter(attr =>
                        Object.prototype.hasOwnProperty.call(
                          _spec,
                          inflection.column(attr),
                        ),
                      )
                      // Loop thru columns and "SQLify" them
                      const sqlColumns = specifiedAttributes.map(attr =>
                        sql.identifier(attr.name),
                      )
                      const sqlRowValues = input[inputFieldName].map(
                        upsertInputRow => {
                          const row = specifiedAttributes.map(attr => {
                            const key = inflection.column(attr)
                            const columnValue = gql2pg(
                              upsertInputRow[key],
                              attr.type,
                              attr.typeModifier,
                            )
                            return columnValue
                          })
                          return row
                        },
                      )

                      const _primaryKeys = primaryKeys.map(
                        key => sql.identifier(key.name).names[0],
                      )
                      const isPrimary = i => _primaryKeys.includes(i.names[0])

                      const conflictUpdateArray = sqlColumns
                        .map(col => {
                          const columnName = sql.identifier(col.names[0])
                          // TODO add "no overwrite with null" options
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
                      const join = fields => sql.join(fields, ', ')

                      const sqlPrimaryKeys = primaryKeys.map(key =>
                        sql.identifier(key.name),
                      )

                      // SQL query for upsert mutations
                      const mutationQuery = sql.query`
                        INSERT INTO ${sqlTableName} (
                          ${join(sqlColumns)}
                        ) VALUES ${join(
                          sqlRowValues.map(row => sql.fragment`(${join(row)})`),
                        )} ON CONFLICT (${join(sqlPrimaryKeys)}) DO UPDATE
                          SET ${join(conflictUpdateArray)} RETURNING *`

                      let rows
                      try {
                        await pgClient.query('SAVEPOINT graphql_mutation')
                        rows = await viaTemporaryTable(
                          pgClient,
                          sql.identifier(table.namespace.name, table.name),
                          mutationQuery,
                          upsertedRowsAlias,
                          query,
                        )
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
                        data: rows,
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
            `Adding upsert batch mutation for ${describePgEntity(
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
      `Adding 'upsert batch' mutation to root mutation`,
    )
  })
}

module.exports = PgMutationUpsertBatchPlugin
