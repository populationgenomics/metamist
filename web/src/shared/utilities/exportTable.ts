// web/src/shared/utilities/exportTable.ts
import Papa from 'papaparse'

export type TableExportConfig = {
    /** Column headers (first row) */
    headerFields: string[]
    /** 2-D array of row data (each row already includes its “row label” as element 0) */
    matrix: string[][]
    /** Any extra Papa Parse unparse options you might want to pass through */
    papaOptions?: Papa.UnparseConfig
}

/**
 * Export a table to CSV or TSV and trigger a download in the browser.
 *
 * @param config   Structure describing the table data
 * @param format   Either `'csv'` or `'tsv'`
 * @param fileName Basename for the generated file (extension is added automatically)
 */
export function exportTable(
    { headerFields, matrix, papaOptions = {} }: TableExportConfig,
    format: 'csv' | 'tsv',
    fileName: string
) {
    const delimiter = format === 'csv' ? ',' : '\t'

    const content = Papa.unparse(
        {
            fields: headerFields,
            data: matrix,
        },
        {
            delimiter,
            newline: '\n',
            quotes: true, // surround every field with quotes for safety
            skipEmptyLines: false,
            ...papaOptions, // allow caller overrides
        }
    )

    const blob = new Blob([content], { type: `text/${format};charset=utf-8;` })
    const url = URL.createObjectURL(blob)

    const link = document.createElement('a')
    link.href = url
    link.setAttribute('download', `${fileName}.${format}`)
    document.body.appendChild(link)
    link.click()
    document.body.removeChild(link)
    URL.revokeObjectURL(url)
}
