import * as React from 'react'
import _ from 'lodash'

import LoadingDucks from '../../shared/components/LoadingDucks/LoadingDucks'
import { AnalysisApi, Project, ProjectApi, ProportionalDateTemporalMethod } from '../../sm-api'
import { Message, Select } from 'semantic-ui-react'
import {
    IStackedAreaByDateChartData,
    StackedAreaByDateChart,
} from '../../shared/components/Graphs/StackedAreaByDateChart'

interface IProportionalDateProjectModel {
    project: string
    percentage: number
    size: number
}

interface IProportionalDateModel {
    date: string
    projects: IProportionalDateProjectModel[]
}

interface ISeqrProportionalMapGraphProps {
    start: string
    end: string
}

const SeqrProportionalMapGraph: React.FunctionComponent<ISeqrProportionalMapGraphProps> = ({
    start,
    end,
}) => {
    const [isLoading, setIsLoading] = React.useState<boolean>(true)
    const [error, setError] = React.useState<string | undefined>()

    const [temporalMethod, setTemporalMethod] = React.useState<ProportionalDateTemporalMethod>(
        ProportionalDateTemporalMethod.SampleCreateDate
    )
    const [projectSelections, setProjectSelections] = React.useState<
        { [key: string]: boolean } | undefined
    >()
    const [allPropMapData, setAllPropMapData] =
        React.useState<{ [m in ProportionalDateTemporalMethod]: IStackedAreaByDateChartData[] }>()

    function updateProjects(projects: { [key: string]: boolean }) {
        const updatedProjects = { ...(projectSelections || {}), ...projects }
        setProjectSelections(updatedProjects)
        loadPropMap(updatedProjects)
    }

    // @ts-ignore
    const allMethods = Object.keys(ProportionalDateTemporalMethod).map(
        (key) => ProportionalDateTemporalMethod[key as keyof typeof ProportionalDateTemporalMethod]
    )

    function loadPropMap(projects: { [key: string]: boolean } = {}) {
        const projectsToSearch = Object.keys(projects).filter((project) => projects[project])

        const api = new AnalysisApi()
        const defaultPropMap = projectsToSearch.reduce((prev, p) => ({ ...prev, [p]: 0 }), {})
        setIsLoading(true)
        setError(undefined)
        api.getProportionateMap(
            start,
            {
                projects: projectsToSearch,
                sequencing_types: [], // searches across all seq types
                temporal_methods: [
                    ProportionalDateTemporalMethod.SampleCreateDate,
                    ProportionalDateTemporalMethod.EsIndexDate,
                ],
            },
            end
        )
            .then((summary) => {
                setIsLoading(false)
                const allGraphData: {
                    [tMethod in ProportionalDateTemporalMethod]: IStackedAreaByDateChartData[]
                } = Object.keys(summary.data).reduce(
                    (prev, tMethod) => ({
                        ...prev,
                        [tMethod]: summary.data[tMethod].map((obj: IProportionalDateModel) => ({
                            date: new Date(obj.date),
                            values: {
                                ...defaultPropMap,
                                ...obj.projects.reduce(
                                    (prev: { [project: string]: number }, projectObj) => ({
                                        ...prev,
                                        // in percentage, rounded to 2 decimal places
                                        [projectObj.project]: projectObj.percentage,
                                    }),
                                    {}
                                ),
                            },
                        })),
                    }),
                    {} as { [tMethod in ProportionalDateTemporalMethod]: IStackedAreaByDateChartData[] }
                )

                // convert end to date if exists, or use current date
                const graphEnd = !!end ? new Date(end) : new Date()

                // If the graph isn't at endDate, add a second entry at the endDate
                // so it finishes at endDate for a better visual graph
                for (const temporalMethod of Object.keys(allGraphData)) {
                    const temporalMethodKey = temporalMethod as ProportionalDateTemporalMethod
                    const values = allGraphData[temporalMethodKey]
                    const lastValue = values[values.length - 1]
                    if (lastValue.date <= graphEnd) {
                        allGraphData[temporalMethodKey].push({
                            date: graphEnd,
                            values: lastValue.values,
                        } as IStackedAreaByDateChartData)
                    }
                }

                setAllPropMapData(allGraphData)
            })
            .catch((er: Error) => {
                // @ts-ignore
                let message = er.response?.data?.description || er?.message
                setError(message)
                setIsLoading(false)
            })
    }

    function getSeqrProjects() {
        const api = new ProjectApi()
        api.getSeqrProjects()
            .then((projects) => {
                const newProjects = projects.data.reduce(
                    (prev: { [project: string]: boolean }, project: Project) => ({
                        ...prev,
                        [project.name!]: true,
                    }),
                    {}
                )
                updateProjects(newProjects)
            })
            .catch((er) => {
                setError(er.message)
                setIsLoading(false)
            })
    }

    // on first load
    React.useEffect(() => {
        if (!projectSelections) {
            getSeqrProjects()
        }
    }, [])

    React.useEffect(() => {
        if (projectSelections) {
            loadPropMap(projectSelections)
        }
    }, [start, end])

    if (!allPropMapData && !isLoading && !error) {
        return <Message negative>No data found</Message>
    }

    const selectedProjects: string[] = projectSelections
        ? _.sortBy(Object.keys(projectSelections).filter((project) => projectSelections[project]))
        : []

    return (
        <>
            <h5>Seqr proportional costs</h5>
            {isLoading && (
                <>
                    <LoadingDucks />
                    <p style={{ textAlign: 'center', marginTop: '5px' }}>
                        <em>This query takes a while...</em>
                    </p>
                </>
            )}
            {error && <Message negative>{error}</Message>}
            <Select
                options={allMethods.map((m) => ({
                    key: m,
                    text: m,
                    value: m,
                }))}
                value={temporalMethod}
                onChange={(e, { value }) => {
                    setTemporalMethod(value as ProportionalDateTemporalMethod)
                }}
            />
            <StackedAreaByDateChart
                keys={selectedProjects}
                data={allPropMapData?.[temporalMethod]}
                start={new Date(start)}
                end={new Date(end)}
                isPercentage={true}
            />
        </>
    )
}

export default SeqrProportionalMapGraph
