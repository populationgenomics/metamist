import * as React from 'react'
import { Button } from 'semantic-ui-react'

import { ExportType, ProjectParticipantGridFilter, WebApi } from '../../sm-api/api'

export const ProjectExportButton: React.FunctionComponent<{
    projectName: string
    filterValues: ProjectParticipantGridFilter
}> = ({ filterValues, projectName }) => {
    const [isDownloading, setIsDownloading] = React.useState(false)
    const [downloadError, setDownloadError] = React.useState<string | undefined>(undefined)

    const download = () => {
        setIsDownloading(true)
        setDownloadError(undefined)
        new WebApi()
            .exportProjectData(ExportType.Csv, projectName, filterValues)
            .then((resp) => {
                setIsDownloading(false)
                const url = window.URL.createObjectURL(new Blob([resp.data]))
                const link = document.createElement('a')
                link.href = url
                const defaultFilename = `project-export-${projectName}.csv`
                link.setAttribute(
                    'download',
                    resp.headers['content-disposition']?.split('=')?.[1] || defaultFilename
                )
                document.body.appendChild(link)
                link.click()
            })
            .catch((er) => setDownloadError(er.message))
    }
    return (
        <>
            <Button loading={isDownloading} onClick={download}>
                Export CSV
            </Button>
            {downloadError && (
                <>
                    <br />
                    <span>Error: {downloadError}</span>
                </>
            )}
        </>
    )
}
