import * as React from 'react'
import _ from 'lodash'

import LoadingDucks from '../../shared/components/LoadingDucks/LoadingDucks'
import { AnalysisApi, Project, ProjectApi, ProportionalDateTemporalMethod } from '../../sm-api'
import { Message, Select } from 'semantic-ui-react'
import { IStackedAreaChartData, StackedAreaChart } from '../../shared/components/Graphs/StackedAreaChart'

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
    const tooltipRef = React.useRef()
    const [hovered, setHovered] = React.useState('')
    const [isLoading, setIsLoading] = React.useState<boolean>(true)
    const [error, setError] = React.useState<string | undefined>()

    const [temporalMethod, setTemporalMethod] = React.useState<ProportionalDateTemporalMethod>(
        ProportionalDateTemporalMethod.SampleCreateDate
    )
    const [projectSelections, setProjectSelections] = React.useState<
        { [key: string]: boolean } | undefined
    >()
    const [graphWidth, setGraphWidth] = React.useState<number>(
        document.getElementById('billing-container')?.clientWidth || 1600
    )
    const [allPropMapData, setAllPropMapData] =
        React.useState<{ [m in ProportionalDateTemporalMethod]: IStackedAreaChartData[] }>()

    // const [propMap, setPropMap] = React.useState<>()

    function updateWindowWidth() {
        setGraphWidth(document.getElementById('billing-container')?.clientWidth || 1600)
    }

    function updateProjects(projects: { [key: string]: boolean }) {
        const updatedProjects = { ...(projectSelections || {}), ...projects }
        setProjectSelections(updatedProjects)
        loadPropMap(updatedProjects)
    }

    // @ts-ignore
    const allMethods = Object.keys(ProportionalDateTemporalMethod).map(
        (key) => ProportionalDateTemporalMethod[key]
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
                updateWindowWidth()
                const allGraphData: {
                    [tMethod in ProportionalDateTemporalMethod]: IStackedAreaChartData[]
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
                    {} as { [tMethod in ProportionalDateTemporalMethod]: IStackedAreaChartData[] }
                )

                // if there's only one element in the list, this breaks the stacked area chart, so add
                // a second entry at 
                // convert end to date if exists, or use current date
                const graphEnd = !!end ? new Date(end) : new Date()
                for (const temporalMethod of Object.keys(allGraphData)) {
                    const temporalMethodKey = temporalMethod as ProportionalDateTemporalMethod;
                    if (allGraphData[temporalMethodKey].length === 1) {
                        allGraphData[temporalMethodKey].push({
                            date: graphEnd,
                            values: allGraphData[temporalMethodKey][0].values,
                        } as IStackedAreaChartData);
                    }
                }

                setAllPropMapData(allGraphData);
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
    window.addEventListener('resize', updateWindowWidth)

    let graphComponent: React.ReactElement | undefined = undefined

    if (allPropMapData) {
        graphComponent = <StackedAreaChart
            keys={selectedProjects}
            data={allPropMapData[temporalMethod]}

            start={new Date(start)}
            end={new Date(end)}
            isPercentage={true}
        />
    }

    return (
        <>
            <h5>Seqr proportional costs</h5>
            {isLoading && <LoadingDucks />}
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
            {graphComponent}
        </>
    )
}

export default SeqrProportionalMapGraph
