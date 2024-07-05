import * as React from 'react'
import { Button, ButtonGroup, Dropdown, Message } from 'semantic-ui-react'

import {
    ExportType,
    MetaSearchEntityPrefix,
    ProjectParticipantGridField,
    ProjectParticipantGridFilter,
    WebApi,
} from '../../sm-api/api'

const extensionFromExportType = (exportType: ExportType) => {
    switch (exportType) {
        case ExportType.Csv:
            return 'csv'
        case ExportType.Tsv:
            return 'tsv'
        case ExportType.Json:
            return 'json'
        default:
            return 'txt'
    }
}

export const ProjectExportButton: React.FunctionComponent<{
    participants_in_query: number
    projectName: string

    filterValues: ProjectParticipantGridFilter
    headerGroups: Record<MetaSearchEntityPrefix, ProjectParticipantGridField[]>
}> = ({ filterValues, projectName, headerGroups, participants_in_query }) => {
    const [exportType, setExportType] = React.useState<ExportType>(ExportType.Csv)
    const [isDownloading, setIsDownloading] = React.useState(false)
    const [downloadError, setDownloadError] = React.useState<string | undefined>(undefined)

    const download = (_exportType: ExportType) => {
        if (!_exportType) return
        setIsDownloading(true)
        setDownloadError(undefined)
        new WebApi()
            .exportProjectParticipants(_exportType, projectName, {
                query: filterValues,
                fields: { fields: headerGroups },
            })
            .then((resp) => {
                setIsDownloading(false)
                let data = resp.data
                if (_exportType == ExportType.Json) {
                    data = JSON.stringify(data)
                }
                const url = window.URL.createObjectURL(new Blob([data]))
                const link = document.createElement('a')
                link.href = url
                const ext = extensionFromExportType(_exportType)
                const defaultFilename = `project-export-${projectName}.${ext}`
                link.setAttribute(
                    'download',
                    resp.headers['content-disposition']?.split('=')?.[1] || defaultFilename
                )
                document.body.appendChild(link)
                link.click()
            })
            .catch((er) => {
                setIsDownloading(false)
                setDownloadError(er.message)
            })
    }

    // onClick={() => download(ExportType.Csv)
    const exportOptions = [
        {
            key: 'csv',
            text: 'CSV',
            value: ExportType.Csv,
        },
        {
            key: 'tsv',
            text: 'TSV',
            value: ExportType.Tsv,
        },
        {
            key: 'json',
            text: 'JSON (all columns)',
            value: ExportType.Json,
        },
    ]

    return (
        <>
            <ButtonGroup>
                <Button loading={isDownloading} onClick={() => download(exportType)}>
                    Export {participants_in_query} participants
                </Button>
                <Dropdown
                    className="button icon"
                    icon="caret down"
                    floating
                    value={exportType}
                    onChange={(e, data) => {
                        setExportType(data.value as ExportType)
                        download(data.value as ExportType)
                    }}
                    options={exportOptions}
                />
            </ButtonGroup>
            {downloadError && (
                <Message error>
                    <br />
                    <span>Error: {downloadError}</span>
                </Message>
            )}
        </>
    )
}
