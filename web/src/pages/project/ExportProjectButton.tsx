import * as React from 'react'
import { Button, Message } from 'semantic-ui-react'

import _ from 'lodash'
import {
    ExportProjectParticipantFields,
    ExportType,
    ProjectParticipantGridFilter,
    WebApi,
} from '../../sm-api/api'
import { MetaSearchEntityPrefix, ProjectGridHeaderGroup } from './ProjectColumnOptions'

export const ProjectExportButton: React.FunctionComponent<{
    participants_in_query: number
    projectName: string

    filterValues: ProjectParticipantGridFilter
    headerGroups: ProjectGridHeaderGroup[]
}> = ({ filterValues, projectName, headerGroups, participants_in_query }) => {
    const [isDownloading, setIsDownloading] = React.useState(false)
    const [downloadError, setDownloadError] = React.useState<string | undefined>(undefined)
    // const keys = Object.keys(headerGroups).reduce((prev, k) => ({...prev, k}), {})

    const download = () => {
        setIsDownloading(true)
        setDownloadError(undefined)

        const hgByCategory = _.keyBy(headerGroups, (hg) => hg.category)

        const fields: ExportProjectParticipantFields = {
            family_keys: hgByCategory[MetaSearchEntityPrefix.F].fields
                .filter((f) => f.isVisible)
                .map((f) => f.name),
            participant_keys: hgByCategory[MetaSearchEntityPrefix.P].fields
                .filter((f) => f.isVisible)
                .map((f) => f.name),
            sample_keys: hgByCategory[MetaSearchEntityPrefix.S].fields
                .filter((f) => f.isVisible)
                .map((f) => f.name),
            sequencing_group_keys: hgByCategory[MetaSearchEntityPrefix.Sg].fields
                .filter((f) => f.isVisible)
                .map((f) => f.name),
            assay_keys: hgByCategory[MetaSearchEntityPrefix.A].fields
                .filter((f) => f.isVisible)
                .map((f) => f.name),
        }
        new WebApi()
            .exportProjectParticipants(ExportType.Csv, projectName, {
                query: filterValues,
                fields: fields,
            })
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
                Export {participants_in_query} participants: CSV
            </Button>
            {downloadError && (
                <Message error>
                    <br />
                    <span>Error: {downloadError}</span>
                </Message>
            )}
        </>
    )
}
