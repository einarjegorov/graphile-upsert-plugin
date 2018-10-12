function PgMutationUpsertBatchPlugin(builder, { pgInflection: inflection }) {
  builder.hook(
    'GraphQLObjectType:fields',
    (
      fields,
      {
        extend,
        getTypeByName,
        newWithHooks,
        parseResolveInfo,
        pgIntrospectionResultsByKind,
        pgSql: sql,
        gql2pg,
        graphql: {
          GraphQLObjectType,
          GraphQLList,
          GraphQLInputObjectType,
          GraphQLNonNull,
          GraphQLString,
        },
        pgQueryFromResolveData: queryFromResolveData,
        pgViaTemporaryTable: viaTemporaryTable,
      },
      { scope: { isRootMutation }, fieldWithHooks },
    ) => {
      if (!isRootMutation) {
        return fields
      }

      return extend(
        fields,
        pgIntrospectionResultsByKind.class
          .filter(table => !!table.namespace)
          .filter(table => table.isSelectable)
          .filter(table => table.isInsertable)
          .filter(table => table.isUpdatable)
          .reduce((memo, table) => {
            const Table = getTypeByName(
              inflection.tableType(table.name, table.namespace.name),
            )

            if (!Table) {
              return memo
            }

            const TableInput = getTypeByName(inflection.inputType(Table.name))
            if (!TableInput) {
              return memo
            }
            const { pluralize } = inflection

            const tableTypeName = inflection.tableType(
              table.name,
              table.namespace.name,
            )

            const tableName = inflection.tableName(
              table.name,
              table.namespace.name,
            )
            const inputFieldName = inflection.pluralize(tableName)

            // Standard input type that 'create' uses
            const InputType = newWithHooks(
              GraphQLInputObjectType,
              {
                name: `Upsert${tableTypeName}BatchInput`,
                description: `All input for the upsert \`${tableTypeName}\` mutation.`,
                fields: {
                  clientMutationId: {
                    description:
                      'An arbitrary string value with no semantic meaning. Will be included in the payload verbatim. May be used to track mutations by the client.',
                    type: GraphQLString,
                  },
                  [inputFieldName]: {
                    description: `The \`${tableTypeName}\` to be upserted by this mutation.`,
                    type: new GraphQLNonNull(new GraphQLList(TableInput)),
                  },
                },
              },
              {
                isPgCreateInputType: false,
                pgInflection: table,
              },
            )

            // Standard payload type that 'create' uses
            const PayloadType = newWithHooks(
              GraphQLObjectType,
              {
                name: `Upsert${tableTypeName}BatchPayload`,
                description: `The output of our \`upsert${tableTypeName}Batch\` mutation.`,
                fields: ({ recurseDataGeneratorsForField }) => {
                  recurseDataGeneratorsForField(tableName)
                  return {
                    clientMutationId: {
                      description:
                        'The exact same `clientMutationId` that was provided in the mutation input, unchanged and unused. May be used by a client to track mutations.',
                      type: GraphQLString,
                    },
                    [inputFieldName]: {
                      description: `The \`${tableTypeName}\` that were upserted by this mutation.`,
                      type: new GraphQLList(Table),
                      resolve(data) {
                        return data.data
                      },
                    },
                  }
                },
              },
              {
                isMutationPayload: true,
                isPgCreatePayloadType: false,
                pgIntrospection: table,
              },
            )

            // Create upsert fields from each introspected table
            const fieldName = `upsert${tableTypeName}Batch`

            memo[fieldName] = fieldWithHooks(
              fieldName,
              ({ getDataFromParsedResolveInfoFragment }) => ({
                description: `Upserts a batch of \`${tableTypeName}\`.`,
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
                  const insertedRowsAlias = sql.identifier(Symbol())
                  const query = queryFromResolveData(
                    insertedRowsAlias,
                    insertedRowsAlias,
                    resolveData,
                    {},
                  )

                  // Store attributes (columns) for easy access
                  const attributes = pgIntrospectionResultsByKind.attribute.filter(
                    attr => attr.classId === table.id,
                  )

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

                  // Loop thru columns and "SQLify" them
                  const sqlColumns = attributes.map(attr =>
                    sql.identifier(attr.name),
                  )
                  let foo = true
                  const sqlRowValues = input[inputFieldName].map(
                    upsertInputRow => {
                      const row = attributes.map(attr => {
                        const fieldName = inflection.column(
                          attr.name,
                          table.name,
                          table.namespace.name,
                        )
                        const column = gql2pg(
                          upsertInputRow[fieldName],
                          attr.type,
                          attr.typeModifier,
                        )
                        return column
                      })
                      return row
                    },
                  )

                  const _primaryKeys = primaryKeys.map(
                    key => sql.identifier(key.name).names[0],
                  )
                  const isPrimary = i => _primaryKeys.includes(i.names[0])

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
                  const join = fields => sql.join(fields, ', ')

                  // no inputs, no upserts
                  if (!sqlRowValues.length) {
                    return {
                      clientMutationId: input.clientMutationId,
                      data: [],
                    }
                  }

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

                  const { rows } = await viaTemporaryTable(
                    pgClient,
                    sqlTableName,
                    mutationQuery,
                    insertedRowsAlias,
                    query,
                  )

                  return {
                    clientMutationId: input.clientMutationId,
                    data: rows,
                  }
                },
              }),
              {},
            )

            return memo
          }, {}),
      )
    },
  )
}

module.exports = PgMutationUpsertBatchPlugin
