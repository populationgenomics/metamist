import React from 'react'
import { ProjectSeqrStats, WebApi, ProjectApi } from '../../sm-api'
import SeqrProjectSelector from './SeqrProjectSelector'

import { Table } from 'semantic-ui-react'

interface SeqrProjectProps {
    id: number
    name: string
}

const SeqrStats: React.FC = () => {
    // Get the list of seqr projects from the project API
    const [seqrProjectNames, setSeqrProjectNames] = React.useState<string[]>([])
    const [seqrProjectIds, setSeqrProjectIds] = React.useState<number[]>([])
    const [selectedProjectNames, setSelectedProjectNames] = React.useState<string[]>([])
    const [selectedProjectIds, setSelectedProjectIds] = React.useState<number[]>([])

    const handleProjectSelected = (projectName: string, isSelected: boolean) => {
        const projectIndex = seqrProjectNames.indexOf(projectName)
        const projectId = seqrProjectIds[projectIndex]

        if (isSelected) {
            setSelectedProjectNames([...selectedProjectNames, projectName])
            setSelectedProjectIds([...selectedProjectIds, projectId])
        } else {
            setSelectedProjectNames(selectedProjectNames.filter((name) => name !== projectName))
            setSelectedProjectIds(selectedProjectIds.filter((id) => id !== projectId))
        }
    }

    React.useEffect(() => {
        new ProjectApi().getSeqrProjects({}).then((resp) => {
            const projects: SeqrProjectProps[] = resp.data
            setSeqrProjectNames(projects.map((project) => project.name))
            setSeqrProjectIds(projects.map((project) => project.id))
        })
    }, [])

    const [responses, setResponses] = React.useState<ProjectSeqrStats[]>([])
    // 0-indexed page number
    const [pageNumber, setPageNumber] = React.useState<number>(0)
    const [pageSize, setPageSize] = React.useState<number>(1)

    React.useEffect(() => {
        if (selectedProjectIds.length > 0) {
            new WebApi().getProjectsSeqrStats({ projects: selectedProjectIds }).then((resp) => {
                setResponses(resp.data)
            })
        }
    }, [selectedProjectIds])

    const pagedResults = responses.slice(pageNumber * pageSize, (pageNumber + 1) * pageSize)

    return (
        <div>
            <h1>Select projects</h1>
            <SeqrProjectSelector
                projectNames={seqrProjectNames}
                projectIds={seqrProjectIds}
                onProjectSelected={handleProjectSelected}
            />
            <select
                onChange={(e) => {
                    setPageSize(parseInt(e.currentTarget.value))
                    setPageNumber(0)
                }}
                value={pageSize}
            >
                {[1, 10, 50, 100].map((value) => (
                    <option value={value} key={`setPageSize-${value}`}>
                        {value}
                    </option>
                ))}
            </select>
            <Table>
                <thead>
                    <tr>
                        <th>Project</th>
                        <th>sequencing_type</th>
                        <th>total_families</th>
                        <th>total_participants</th>
                        <th>total_samples</th>
                        <th>total_sequencing_grouss</th>
                        <th>total_crams</th>
                        <th>latest_es_index_id</th>
                        <th>total_sgs_in_latest_es_index</th>
                        <th>latest_annotate_dataset_id</th>
                        <th>total_sgs_in_latest_annotate_dataset</th>
                        <th>links</th>
                    </tr>
                </thead>
                <tbody>
                    {pagedResults.map((ss) => (
                        <tr key={ss.project}>
                            <td>{ss.dataset}</td>
                            <td>{ss.sequencing_type}</td>
                            <td>{ss.total_families}</td>
                            <td>{ss.total_participants}</td>
                            <td>{ss.total_samples}</td>
                            <td>{ss.total_sequencing_groups}</td>
                            <td>{ss.total_crams}</td>
                            <td>{ss.latest_es_index_id}</td>
                            <td>{ss.total_sgs_in_latest_es_index}</td>
                            <td>{ss.latest_annotate_dataset_id}</td>
                            <td>{ss.total_sgs_in_latest_annotate_dataset}</td>
                            <td></td>
                        </tr>
                    ))}
                </tbody>
            </Table>
            {
                <button disabled={pageNumber === 0} onClick={() => setPageNumber(pageNumber - 1)}>
                    Previous page
                </button>
            }
            Page {pageNumber + 1}
            {(pageNumber + 1) * pageSize < responses.length && (
                <button onClick={() => setPageNumber(pageNumber + 1)}>Next page</button>
            )}
        </div>
    )
}

export default SeqrStats
