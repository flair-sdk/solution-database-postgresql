import { Config as DrizzleConfig } from 'drizzle-kit'
import { PgColumn, PgTable, getTableConfig } from 'drizzle-orm/pg-core'
import {
  AppError,
  EnricherEngine,
  FieldType,
  Schema,
  SolutionContext,
  SolutionDefinition,
  SolutionScriptFunction,
} from 'flair-sdk'

import { DatabaseSyncEnricherParameters } from './types.js'

export type FieldMapping = {
  entityType: string | '*'
  sourceField: string
  targetField: string | null
}

export type Config = {
  schema: string | string[] | { drizzleConfig: string }
  instance?: string
  connectionUri?: string
  tableNamePrefix?: string | false
  username?: string
  password?: string
  maxRetries?: string | number
  bufferFlushInterval?: string
  fieldMappings?: FieldMapping[]
}

const definition: SolutionDefinition<Config> = {
  prepareManifest: async (context, config, manifest) => {
    if (!config.schema || !Array.isArray(config.schema)) {
      throw new AppError({
        code: 'InvalidConfigError',
        message:
          'in postgresql solution "schema" field must be a string or an array of strings',
        details: {
          config,
        },
      })
    }

    const { mergedSchema, entityIdMappings } = await loadSchema(
      context,
      config.schema,
    )
    let streamingSql = `SET 'execution.runtime-mode' = 'STREAMING';`
    let batchSql = `SET 'execution.runtime-mode' = 'BATCH';`

    for (const schemaName in mergedSchema) {
      try {
        const prefix =
          config.tableNamePrefix === undefined ||
          config.tableNamePrefix === null ||
          config.tableNamePrefix === false
            ? ''
            : config.tableNamePrefix || 'entity_'

        const entityType = schemaName.startsWith(prefix)
          ? schemaName.slice(prefix.length)
          : schemaName
        const tableName = schemaName.startsWith(prefix)
          ? schemaName
          : `${prefix}${schemaName}`

        let idField = entityIdMappings[schemaName] ?? 'entityId'
        const removedFields = []

        const fieldsList = Object.entries(mergedSchema[schemaName])
          .map(([fieldName, fieldType]) => {
            const fieldMapping = config.fieldMappings?.find(
              (mapping) =>
                (mapping.entityType === '*' ||
                  mapping.entityType.replace(prefix, '') === entityType) &&
                mapping.sourceField === fieldName,
            )

            if (fieldMapping) {
              if (!fieldMapping.targetField) {
                if (fieldMapping.sourceField === 'entityId') {
                  throw new Error(
                    `entityId field cannot be removed as that is primary key for "${schemaName}" field mapping: ${JSON.stringify(
                      fieldMapping,
                    )}`,
                  )
                }
                removedFields.push(fieldName)
                return null
              }
              if (fieldMapping.sourceField === 'entityId') {
                idField = fieldMapping.targetField
              }
              return [fieldMapping.targetField, fieldType]
            }
            return [fieldName, fieldType]
          })
          .filter(Boolean)

        const sourceFieldsSql = Object.entries(mergedSchema[schemaName])
          .map(([fieldName, fieldType]) => {
            if (removedFields.includes(fieldName)) {
              return null
            }
            if (idField === fieldName) {
              return `  \`entityId\` ${getSqlType(fieldType as FieldType)}`
            } else {
              return `  \`${fieldName}\` ${getSqlType(fieldType as FieldType)}`
            }
          })
          .filter(Boolean)
          .join(',\n')

        const sinkFieldsSql = fieldsList
          .map(([fieldName, fieldType]) => {
            return `  \`${fieldName}\` ${getSqlType(fieldType as FieldType)}`
          })
          .join(',\n')

        const insertSelectFields = Object.entries(mergedSchema[schemaName])
          .map(([fieldName, fieldType]) => {
            if (removedFields.includes(fieldName)) {
              return null
            }
            if (idField === fieldName) {
              return `TRY_CAST(\`entityId\` AS STRING)`
            } else {
              return `TRY_CAST(\`${fieldName}\` AS ${getSqlType(fieldType)})`
            }
          })
          .filter(Boolean)
          .join(', \n')

        if (!mergedSchema[schemaName][idField]) {
          throw new Error(
            `identifier field '${idField}' is required, but missing for "${schemaName}" in "${config.schema}"`,
          )
        }
        if (mergedSchema[schemaName][idField] !== FieldType.STRING) {
          throw new Error(
            `identifier field '${idField}' must be of type STRING, but is of type "${mergedSchema[schemaName][idField]}" for "${schemaName}" in "${config.schema}"`,
          )
        }

        streamingSql += `
---
--- ${schemaName}
---
CREATE TABLE source_${schemaName} (
${sourceFieldsSql},
  PRIMARY KEY (\`entityId\`) NOT ENFORCED
) WITH (
  'connector' = 'stream',
  'mode' = 'cdc',
  'namespace' = '{{ namespace }}',
  'entity-type' = '${entityType}',
  'scan.startup.mode' = 'timestamp',
  'scan.startup.timestamp-millis' = '{{ chrono("2 hours ago") * 1000 }}'
);

CREATE TABLE sink_${schemaName} (
${sinkFieldsSql},
  PRIMARY KEY (\`${idField}\`) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = '${config.connectionUri || '{{ secret("postgresql.uri") }}'}',
  'table-name' = '${tableName}',
  'username' = '${config.username || 'postgres'}',
  'password' = '${config.password || '{{ secret("postgresql.password") }}'}',
  'sink.max-retries' = '${config.maxRetries || '10'}',
  'sink.buffer-flush.interval' = '${config.bufferFlushInterval || '60s'}'
);

INSERT INTO sink_${schemaName} SELECT ${insertSelectFields} FROM source_${schemaName};
`

        const fields = Object.entries(mergedSchema[schemaName])
        let timestampField = fields.find(
          ([fieldName, _fieldType]) => fieldName === 'blockTimestamp',
        )?.[0]
        if (!timestampField) {
          timestampField = fields.find(([fieldName, _fieldType]) =>
            fieldName?.toLowerCase().includes('timestamp'),
          )?.[0]
        }

        batchSql += `
---
--- ${schemaName}
---
CREATE TABLE source_${schemaName} (
${sourceFieldsSql},
  PRIMARY KEY (\`entityId\`) NOT ENFORCED
) WITH (
  'connector' = 'database',
  'mode' = 'read',
  'namespace' = '{{ namespace }}',
  'entity-type' = '${entityType}'${
          timestampField
            ? `,
  'scan.partition.num' = '10',
  'scan.partition.column' = '${timestampField}',
  'scan.partition.lower-bound' = '{{ chrono(fromTimestamp | default("01-01-2020 00:00 UTC")) }}',
  'scan.partition.upper-bound' = '{{ chrono(toTimestamp | default("now")) }}'
  `
            : ''
        }
);

CREATE TABLE sink_${schemaName} (
${sinkFieldsSql},
  PRIMARY KEY (\`${idField}\`) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = '${config.connectionUri || '{{ secret("postgresql.uri") }}'}',
  'table-name' = '${tableName}',
  'username' = '${config.username || 'postgres'}',
  'password' = '${config.password || '{{ secret("postgresql.password") }}'}',
  'sink.max-retries' = '${config.maxRetries || '10'}',
  'sink.buffer-flush.interval' = '${config.bufferFlushInterval || '60s'}'
);

INSERT INTO sink_${schemaName} SELECT ${insertSelectFields} FROM source_${schemaName};
`
      } catch (e: any) {
        throw AppError.causedBy(e, {
          code: 'ManifestPreparationError',
          message: 'Failed to prepare manifest for user-defined entity',
          details: {
            schemaName,
          },
        })
      }
    }

    if (!manifest.enrichers?.length) {
      manifest.enrichers = []
    }

    const instance = config.instance || 'default'

    context.writeStringFile(
      `database/postgresql-${instance}/streaming.sql`,
      streamingSql,
    )
    context.writeStringFile(
      `database/postgresql-${instance}/batch.sql`,
      batchSql,
    )

    manifest.enrichers.push(
      {
        id: `database-postgresql-${instance}-streaming`,
        engine: EnricherEngine.Flink,
        size: 'small',
        inputSql: `database/postgresql-${instance}/streaming.sql`,
      },
      {
        id: `database-postgresql-${instance}-batch`,
        engine: EnricherEngine.Flink,
        size: 'small',
        inputSql: `database/postgresql-${instance}/batch.sql`,
      },
    )

    return manifest
  },
  registerScripts: (
    context,
    config,
  ): Record<string, SolutionScriptFunction> => {
    const instance = config.instance || 'default'
    return {
      'database-manual-full-sync': {
        run: async (params: DatabaseSyncEnricherParameters) => {
          await context.runCommand('enricher:trigger', [
            `database-postgresql-${instance}-batch`,
            ...(params?.fromTimestamp
              ? ['-p', `fromTimestamp='${params.fromTimestamp}'`]
              : []),
            ...(params?.toTimestamp
              ? ['-p', `toTimestamp='${params.toTimestamp}'`]
              : []),
            ...(params?.autoApprove ? ['--auto-approve'] : []),
          ])
        },
      },
    }
  },
  registerHooks: async (context, config) => {
    let anyNonDrizzleSchema = false
    if (typeof config.schema === 'string') {
      anyNonDrizzleSchema = true
    } else if (Array.isArray(config.schema)) {
      anyNonDrizzleSchema = config.schema.some(
        (schema) => typeof schema === 'string',
      )
    }

    return [
      ...(anyNonDrizzleSchema
        ? ([
            {
              for: 'pre-deploy',
              id: 'infer-schema',
              title: 'infer flink schema (optional)',
              run: async (params?: { autoApprove?: boolean }) => {
                await context.runCommand('util:infer-schema', [
                  ...(params?.autoApprove ? ['--auto-approve'] : []),
                ])
              },
            },
            {
              for: 'pre-deploy',
              id: 'deploy-streaming',
              title: 'configure real-time sync (optional)',
              run: async (params?: { autoApprove?: boolean }) => {
                await context.runCommand('deploy', [
                  '--skip-hooks',
                  '--do-not-exit',
                  ...(params?.autoApprove ? ['--auto-approve'] : []),
                ])
              },
            },
          ] as const)
        : []),
      {
        for: 'pre-deploy',
        id: 'infer-drizzle-schema',
        title: 'infer drizzle schema (optional)',
        run: async (params?: { autoApprove?: boolean }) => {
          await context.runCommand('util:infer-drizzle-schema', [
            ...(params?.autoApprove ? ['--auto-approve'] : []),
          ])
        },
      },
      {
        for: 'pre-deploy',
        id: 'postgresql-full-sync',
        title: 'one-off historical sync for postgresql (optional)',
        run: async (params?: { autoApprove?: boolean }) => {
          await context.runCommand('script', [
            'database-manual-full-sync',
            JSON.stringify(params || {}),
          ])
        },
      },
    ]
  },
}

export default definition

async function loadSchema(
  context: SolutionContext<Config>,
  schemas: Config['schema'],
): Promise<{
  mergedSchema: Schema
  entityIdMappings: Record<string, string>
}> {
  const arrayedSchemas = Array.isArray(schemas) ? schemas : [schemas]
  const files: (string | { drizzleConfig: string })[] =
    arrayedSchemas.flatMap<any>((schema) => {
      return typeof schema === 'object' ? schema : context.glob(schema)
    })

  if (!files.length) {
    console.warn(`No schema files found in: ${arrayedSchemas.join(' ')}`)
  }

  const mergedSchema: Schema = {}
  const entityIdMappings: Record<string, string> = {}

  for (const file of files) {
    try {
      if (typeof file === 'object') {
        if (file && 'drizzleConfig' in file) {
          await loadSchemaFromDrizzleConfig(
            context,
            file,
            mergedSchema,
            entityIdMappings,
          )
        } else {
          throw new AppError({
            code: 'InvalidSchemaError',
            message:
              'Object schema definition must contain a "drizzleConfig" key',
            details: {
              file,
            },
          })
        }
      } else if (typeof file === 'string') {
        await loadSchemaFromYaml(context, file, mergedSchema)
      } else {
        throw new AppError({
          code: 'InvalidSchemaError',
          message: 'Schema must be a string or object',
          details: {
            file,
          },
        })
      }
    } catch (e: any) {
      throw AppError.causedBy(e, {
        code: 'SchemaLoadError',
        message: 'Failed to load schema file',
        details: {
          file,
        },
      })
    }
  }

  return { mergedSchema, entityIdMappings }
}

async function loadSchemaFromYaml(
  context: SolutionContext<Config>,
  file: string,
  mergedSchema: Schema,
) {
  const schema = await context.readYamlFile<Schema>(file)

  if (!schema || typeof schema !== 'object') {
    throw new AppError({
      code: 'InvalidSchemaError',
      message: 'Schema must be an object defined in YAML format',
      details: {
        file,
      },
    })
  }

  for (const [type, fields] of Object.entries(schema)) {
    if (!fields || typeof fields !== 'object') {
      throw new AppError({
        code: 'InvalidSchemaError',
        message: 'Fields for entitiy schema must be an object',
        details: {
          entityType: type,
          file,
        },
      })
    }

    if (mergedSchema[type]) {
      throw new AppError({
        code: 'DuplicateSchemaError',
        message: 'Entity type is already defined in another schema',
        details: {
          entityType: type,
          file,
        },
      })
    }

    mergedSchema[type] = fields
  }
}

function getSqlType(fieldType: FieldType) {
  switch (fieldType) {
    case FieldType.STRING:
      return 'STRING'
    case FieldType.INT256:
      return 'STRING'
    case FieldType.INT64:
      return 'BIGINT'
    case FieldType.FLOAT8:
      return 'DOUBLE'
    case FieldType.BOOLEAN:
      return 'BOOLEAN'
    case FieldType.ARRAY:
      return 'STRING'
    case FieldType.OBJECT:
      return 'STRING'
    default:
      throw new Error(
        `Unsupported field type: ${fieldType} select from: ${Object.values(
          FieldType,
        ).join(', ')}`,
      )
  }
}

async function loadSchemaFromDrizzleConfig(
  context: SolutionContext<Config>,
  file: { drizzleConfig?: string },
  mergedSchema: Schema,
  entityIdMappings: Record<string, string>,
) {
  if (!file.drizzleConfig) {
    throw new AppError({
      code: 'InvalidSchemaError',
      message:
        'Schema object must contain a valid "drizzleConfig" key path to the typescript config file',
      details: {
        file,
      },
    })
  }

  const drizzleConfig: DrizzleConfig = (
    await (context as any).importModule(file.drizzleConfig)
  ).default.default

  const schemaPaths = Array.isArray(drizzleConfig.schema)
    ? drizzleConfig.schema
    : [drizzleConfig.schema]
  for (const schemaPath of schemaPaths) {
    const schemaModule = (await (context as any).importModule(
      schemaPath,
    )) as Record<string, PgTable>
    if (!schemaModule || typeof schemaModule !== 'object') {
      throw new AppError({
        code: 'InvalidSchemaError',
        message: 'Schema file path does not look like a typescript module',
        details: {
          file,
        },
      })
    }

    for (const [_, table] of Object.entries(schemaModule)) {
      if (table?.constructor?.name === 'PgTable') {
        const fields = Object.keys(table as any).filter(
          (field) => 'name' in table[field] && 'columnType' in table[field],
        )
        const cfg = getTableConfig(table)
        const tableName = cfg.name
        mergedSchema[tableName] = fields.reduce((acc, field) => {
          const column = table[field] as PgColumn
          if (column.primary && column.name !== 'entityId') {
            entityIdMappings[tableName] = column.name
          }
          return {
            ...acc,
            [field]: mapPgTypeToFieldType(column),
          }
        }, {} as Record<string, FieldType>)
      }
    }
  }
}

function mapPgTypeToFieldType(column: PgColumn): FieldType {
  switch (column.columnType) {
    case 'PgVarchar':
    case 'PgText':
    case 'PgJson':
      return FieldType.STRING
    case 'PgNumeric':
      return FieldType.INT256
    case 'PgDoublePrecision':
      return FieldType.FLOAT8
    case 'PgInt':
    case 'PgInteger':
    case 'PgSmallInt':
    case 'PgBigInt53':
    case 'PgBigInt64':
    case 'PgReal':
      return FieldType.INT64
    case 'PgBoolean':
      return FieldType.BOOLEAN
    case 'PgTimestamp':
      return FieldType.BOOLEAN
    default:
      throw new Error(
        `Field type '${column.columnType}' is not mapped yet, if you need it, ping our engineers`,
      )
  }
}
