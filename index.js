function PgMutationUpsertPlugin(builder, { pgExtendedTypes, pgInflection: inflection }) {

  // In order to build this mutation, we need some stuff from graphile-build-pg
  // Since these functions are not exported, we need to include the files.

  // NOTE: Currently this plugin requires Node version 8 or newer to work!
  // TODO: Detect node version and fetch appropriate files!

  const queryFromResolveData = require(`${__dirname}/node_modules/graphile-build-pg/node8plus/queryFromResolveData.js`).default;
  const viaTemporaryTable = require(`${__dirname}/node_modules/graphile-build-pg/node8plus/plugins/viaTemporaryTable.js`).default;

  builder.hook("GraphQLObjectType:fields", (fields, build, context) => {
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
        GraphQLString
      },
      pgColumnFilter,
      inflection,
      pgQueryFromResolveData: queryFromResolveData,
      pgOmit: omit,
      pgViaTemporaryTable: viaTemporaryTable
    } = build;
    const {
      scope: { isRootMutation },
      fieldWithHooks
    } = context;
    if (!isRootMutation) {
      return fields;
    }

     return extend(fields, pgIntrospectionResultsByKind.class.filter(table => !!table.namespace).filter(table => table.isSelectable).filter(table => table.isInsertable).filter(table => table.isUpdatable).reduce((memo, table) => {
      const Table = pgGetGqlTypeByTypeIdAndModifier(table.type.id, null);
      if (!Table) {
        return memo;
      }

      const TableInput = pgGetGqlInputTypeByTypeIdAndModifier(table.type.id, null);
      if (!TableInput) {
          return memo;
      }
      const tableTypeName = inflection.tableType(table);
            // Standard input type that 'create' uses
      const InputType = newWithHooks(
              GraphQLInputObjectType,
              {
                name: `Upsert${tableTypeName}Input`,
                description: `All input for the upsert \`${tableTypeName}\` mutation.`,
                fields: {
                  clientMutationId: {
                    description:
                      "An arbitrary string value with no semantic meaning. Will be included in the payload verbatim. May be used to track mutations by the client.",
                    type: GraphQLString,
                  },
                  ...(TableInput ? {
                      [inflection.tableFieldName(table)]: {
                        description: `The \`${tableTypeName}\` to be upserted by this mutation.`,
                        type: new GraphQLNonNull(TableInput),
                      },
                  }: null)
                },
              },
              {
                isPgCreateInputType: false,
                pgInflection: table
              }
            );

            // Standard payload type that 'create' uses
            const PayloadType = newWithHooks(
              GraphQLObjectType,
              {
                name: `Upsert${tableTypeName}Payload`,
                description: `The output of our upsert \`${tableTypeName}\` mutation.`,
                fields: ({ recurseDataGeneratorsForField }) => {
                  const tableName = inflection.tableFieldName(table);
                  recurseDataGeneratorsForField(tableName);
                  return {
                    clientMutationId: {
                      description:
                        "The exact same `clientMutationId` that was provided in the mutation input, unchanged and unused. May be used by a client to track mutations.",
                      type: GraphQLString,
                    },
                    [tableName]: {
                      description: `The \`${tableTypeName}\` that was upserted by this mutation.`,
                      type: Table,
                      resolve(data) {
                        return data.data;
                      },
                    },
                  };
                },
              },
              {
                isMutationPayload: true,
                isPgCreatePayloadType: false,
                pgIntrospection: table
              }
            );

            // Create upsert fields from each introspected table
            const fieldName = `upsert${tableTypeName}`;

            memo[fieldName] = fieldWithHooks(fieldName, context => {
			 const { getDataFromParsedResolveInfoFragment } = context;
			 return {
              description: `Upserts a single \`${tableTypeName}\`.`,
              type: PayloadType,
              args: {
                input: {
                  type: new GraphQLNonNull(InputType)
                }
              },
              async resolve(data, { input }, { pgClient }, resolveInfo) {
                const parsedResolveInfoFragment = parseResolveInfo(
                  resolveInfo
                );
                const resolveData = getDataFromParsedResolveInfoFragment(
                  parsedResolveInfoFragment,
                  PayloadType
                );
                const insertedRowAlias = sql.identifier(Symbol());
                const query = queryFromResolveData(
                  insertedRowAlias,
                  insertedRowAlias,
                  resolveData,
                  {}
                );

                const sqlColumns = [];
                const sqlValues = [];
                const inputData = input[inflection.tableFieldName(table)];

                // Store attributes (columns) for easy access
                const attributes = pgIntrospectionResultsByKind.attribute
                  .filter(attr => attr.classId === table.id);

                // Figure out the pkey constraint
                const primaryKeyConstraint = pgIntrospectionResultsByKind.constraint
                  .filter(con => con.classId === table.id)
                  .filter(con => con.type === "p")[0];

                // Figure out to which column that pkey constraint belongs to
                const primaryKeys =
                  primaryKeyConstraint &&
                  primaryKeyConstraint.keyAttributeNums.map(
                    num => attributes.filter(attr => attr.num === num)[0]
                  );

                // Loop thru columns and "SQLify" them
                attributes.forEach(attr => {
                    const fieldName = inflection.column(attr);
                    const val = inputData[fieldName];
                    if (Object.prototype.hasOwnProperty.call(inputData, fieldName)) {
                      sqlColumns.push(sql.identifier(attr.name));
                      sqlValues.push(gql2pg(val, attr.type, attr.typeModifier));
                    }
                  });

                // Construct a array in case we need to do an update on conflict
                const conflictUpdateArray = sqlColumns.map(col =>
                  sql.query`${sql.identifier(col.names[0])} = excluded.${sql.identifier(col.names[0])}`
                );

                // SQL query for upsert mutations
                const mutationQuery = sql.query`
                  insert into ${sql.identifier(
                    table.namespace.name,
                    table.name
                  )} ${sqlColumns.length
                  ? sql.fragment`(
                      ${sql.join(sqlColumns, ", ")}
                    ) values(${sql.join(sqlValues, ", ")})
                    ON CONFLICT (${sql.identifier(primaryKeys[0].name)}) DO UPDATE
                    SET ${sql.join(conflictUpdateArray, ", ")}`
                  : sql.fragment`default values`} returning *`;

				const rows = await viaTemporaryTable(pgClient, sql.identifier(table.namespace.name, table.name), mutationQuery, insertedRowAlias, query);
				row = rows[0];

                return {
                  clientMutationId: input.clientMutationId,
                  data: row,
                };

              },
            };
      }, {
        pgFieldIntrospection: table,
        isPgCreateMutationField: false
      });
      return memo;
  }, {}))
 });
}

module.exports = PgMutationUpsertPlugin;

