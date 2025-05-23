import * as duckdb from '@duckdb/duckdb-wasm'
import { Table, TypeMap } from 'apache-arrow'
import { AxiosResponse } from 'axios'
import { useEffect, useState } from 'react'
import { ParticipantApi, SampleApi } from '../../../sm-api'

type TableInfo = {
    name: string
    columns: Array<{
        columnName: string
        columnType: string
    }>
}

type TableSetupStatus =
    | {
          name: string
          status: 'success'
          tableInfo: TableInfo
      }
    | {
          name: string
          status: 'error'
          errorMessage: string
      }

type DbSetupStatus =
    | {
          status: 'loading'
      }
    | {
          status: 'success'
          tableSetupStatus: TableSetupStatus[]
      }
    | {
          status: 'error'
          errorMessage: string
      }

type DbQueryResult =
    | {
          status: 'loading'
      }
    | {
          status: 'success'
          data: Table<TypeMap>
      }
    | {
          status: 'error'
          errorMessage: string
      }

async function handleParquetResponse(
    respFuture: Promise<AxiosResponse<ArrayBuffer | void, unknown>>
): Promise<Uint8Array | undefined> {
    const resp = await respFuture
    if (resp.status === 404 || !resp.data) return
    return new Uint8Array(resp.data)
}

const TABLES: Record<string, (project: string) => Promise<Uint8Array | void>> = {
    sample: async (project: string) =>
        handleParquetResponse(
            new SampleApi().exportSamples(project, {
                responseType: 'arraybuffer',
            })
        ),
    participant: async (project: string) =>
        handleParquetResponse(
            new ParticipantApi().exportParticipants(project, {
                responseType: 'arraybuffer',
            })
        ),
}

// The below code handles the duckdb-wasm setup,
// @see here for details: https://duckdb.org/docs/api/wasm/instantiation
// This used the jsdeliver method as the wasm files are >32MB which is above
// the limit of response size for a cloud run function.
const JSDELIVR_BUNDLES = duckdb.getJsDelivrBundles()

let databaseProject: string | null = null
let dbFuture: Promise<duckdb.AsyncDuckDB> | null = null
let buildTablesFuture: Promise<TableSetupStatus[]> | null = null

async function initializeDatabase() {
    // Select a bundle based on browser checks
    const bundle = await duckdb.selectBundle(JSDELIVR_BUNDLES)

    const worker_url = URL.createObjectURL(
        new Blob([`importScripts("${bundle.mainWorker!}");`], { type: 'text/javascript' })
    )

    // Instantiate the asynchronus version of DuckDB-wasm
    const worker = new Worker(worker_url)
    const logger = new duckdb.VoidLogger()
    const db = new duckdb.AsyncDuckDB(logger, worker)
    await db.instantiate(bundle.mainModule, bundle.pthreadWorker)

    URL.revokeObjectURL(worker_url)

    await db.open({
        path: ':memory:',
        query: {
            castBigIntToDouble: true,
            castDecimalToDouble: true,
            castDurationToTime64: true,
            castTimestampToDate: true,
            queryPollingInterval: 1000,
        },
    })
    return db
}

// Add the table into duckdb from the parquet data blob.
// Pulls out the table info and returns it along with the name
async function buildTable(
    project: string,
    db: duckdb.AsyncDuckDB,
    conn: duckdb.AsyncDuckDBConnection,
    tableName: string,
    getData: (project: string) => Promise<Uint8Array | void>
): Promise<TableInfo | undefined> {
    await conn.query(`DROP TABLE IF EXISTS ${tableName}`)
    const data = await getData(project)

    if (!data) return undefined

    await db.registerFileBuffer(`${tableName}.parquet`, data)

    await conn.query(`
        CREATE TABLE ${tableName} AS
            SELECT * FROM '${tableName}.parquet'
    `)

    const columnInfoResp = await conn.query(`SHOW table ${tableName}`)
    const columnInfo: Array<{ column_name: string; column_type: string }> = columnInfoResp
        .toArray()
        .map((row) => row.toJSON())

    return {
        name: tableName,
        columns: columnInfo.map((row) => ({
            columnName: row.column_name,
            columnType: row.column_type,
        })),
    }
}

// Loop through the tables and add them to the database in parallel
async function buildTables(
    dbFuture: Promise<duckdb.AsyncDuckDB>,
    project: string
): Promise<TableSetupStatus[]> {
    const db = await dbFuture

    const c = await db.connect()

    const tableResults = await Promise.all(
        Object.entries(TABLES).map(async ([name, getData]) => {
            try {
                const tableInfo = await buildTable(project, db, c, name, getData)

                if (!tableInfo)
                    return {
                        name,
                        status: 'error' as const,
                        errorMessage: 'Table has no data',
                    }

                return {
                    name,
                    status: 'success' as const,
                    tableInfo,
                }
            } catch (err) {
                const message = err instanceof Error ? err.message : 'Unknown error'
                return { name, status: 'error' as const, errorMessage: message }
            }
        })
    )

    await c.close()

    return tableResults
}

// Set up the database for the project, including building the tables.
// All this work is cached, and will only be redone when changing projects
async function setup(project: string) {
    // Save what the last value of databaseProject was before updating it
    const previousProject = databaseProject
    databaseProject = project
    // Init the db, but don't wait for it to complete yet
    dbFuture = dbFuture || initializeDatabase()

    // If a project has not been set, or the project has changed then build the tables
    if (!previousProject || previousProject !== project || !buildTablesFuture) {
        buildTablesFuture = buildTables(dbFuture, project)
    }

    // Make sure these are finished before returning
    const db = await dbFuture
    const tableList = await buildTablesFuture
    return { db, tableList }
}

// Handle execution of a query, will set up the database if needed
async function executeQuery(project: string, query: string) {
    const { db } = await setup(project)
    const c = await db.connect()
    const result = await c.query(query)
    await c.close()
    return result
}

// Hook to get the setup status of the project database
export function useProjectDbSetup(project: string | undefined) {
    const [setupStatus, setSetupStatus] = useState<DbSetupStatus>()
    useEffect(() => {
        if (!project) return
        let ignore = false
        setSetupStatus({ status: 'loading' })
        setup(project)
            .then(({ tableList }) => {
                if (ignore) return
                setSetupStatus({ tableSetupStatus: tableList, status: 'success' })
            })
            .catch((e) => {
                if (ignore) return
                setSetupStatus({ status: 'error', errorMessage: e.message })
            })

        return () => {
            ignore = true
        }
    }, [project])

    return setupStatus
}

// Hook to run a query on the project database
export function useProjectDbQuery(project: string, query: string) {
    const [result, setResult] = useState<DbQueryResult>()

    useEffect(() => {
        let ignore = false
        setResult({ status: 'loading' })
        executeQuery(project, query)
            .then((data) => {
                if (ignore) return
                setResult({ status: 'success', data })
            })
            .catch((e) => {
                if (ignore) return
                setResult({ status: 'error', errorMessage: e.message })
            })

        return () => {
            ignore = true
        }
    }, [project, query])

    return result
}
