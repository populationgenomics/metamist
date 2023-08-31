import * as React from 'react'
import _ from 'lodash';

import { AnalysisApi, Project, ProjectApi } from '../../sm-api';
import LoadingDucks from '../../shared/components/LoadingDucks/LoadingDucks';
import { Container, Form, Message } from 'semantic-ui-react';

import { scaleLinear, zoom, select, pointer, brushX, stack } from 'd3'

import {
    AreaChart,
    CartesianGrid,
    XAxis,
    YAxis,
    Tooltip,
    Area,
    ResponsiveContainer,
    Legend,

} from 'recharts'

const chartColours = [
    "#8884d8", // purple
    "#82ca9d", // green
    "#ffc658", // yellow
    "#ff7300", // orange
    "#000000", // black
    "#00ff00", // lime
    "#ff0000", // red
    "#0000ff", // blue
    "#00ffff", // aqua
    "#ff00ff", // fuchsia
    "#008000", // green
    "#800080", // purple
    "#808000", // olive
    "#800000", // maroon
    "#008080", // teal
    "#000080", // navy
    "#808080", // gray
    "#c0c0c0", // silver
]

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
const SeqrProportionalMapGraph: React.FunctionComponent<ISeqrProportionalMapGraphProps> = ({ start, end }) => {

    const [isLoading, setIsLoading] = React.useState<boolean>(true)
    const [error, setError] = React.useState<string | undefined>()
    const [showProjectSelector, setShowProjectSelector] = React.useState<boolean>(false)
    const [sequencingType, setSequencingType] = React.useState<string>('genome')
    const [projectSelections, setProjectSelections] = React.useState<{ [key: string]: boolean } | undefined>()
    const [propMap, setPropMap] = React.useState<{ [key: string]: any }[] | undefined>()

    function updateProjects(projects: { [key: string]: boolean }) {
        const updatedProjects = { ...(projectSelections || {}), ...projects }
        setProjectSelections(updatedProjects)
        loadPropMap(updatedProjects)
    }

    function loadPropMap(projects: { [key: string]: boolean } = {}) {

        const projectsToSearch = Object.keys(projects).filter((project) => projects[project])

        const api = new AnalysisApi()
        const defaultPropMap = projectsToSearch.reduce((prev, p) => ({ ...prev, [p]: 0 }), {})
        api.getProportionateMap(sequencingType, projectsToSearch, start, end).then((summary) => {
            setIsLoading(false)

            const graphData = summary.data.map((obj: IProportionalDateModel) => ({
                date: obj.date,
                // ...defaultPropMap,
                ...obj.projects.reduce((prev: { [project: string]: Number }, projectObj) => ({
                    ...prev,
                    // in percentage, rounded to 2 decimal places
                    [projectObj.project]: _.round(projectObj.percentage * 100, 2)

                }), {})
            }))
            let projectsToSee = new Set(projectsToSearch)
            for (let index = 1; index < graphData.length; index++) {
                const graphObj = graphData[index];
                if (projectsToSearch.length == 0) continue;
                // make sure the entry BEFORE a project is visible,
                // it's set to 0 to make the graph prettier
                for (const project of projectsToSee) {
                    if (project in graphObj) {
                        projectsToSee.delete(project)
                        graphData[index - 1][project] = 0
                    }
                    if (projectsToSee.size == 0) break;
                }
            }
            setPropMap(graphData)
        }).catch(er => {
            setError(er.message)
            setIsLoading(false)
        })
    }

    function getSeqrProjects() {
        const api = new ProjectApi()
        api.getSeqrProjects().then((projects) => {
            const newProjects = projects.data.reduce(
                (prev: { [project: string]: boolean }, project: Project) => ({ ...prev, [project.name!]: true }), {}
            )
            updateProjects(newProjects)
        }).catch(er => {
            setError(er.message)
            setIsLoading(false)
        })
    }

    React.useEffect(() => {
        getSeqrProjects()
    }, [])

    const selectedProjects: string[] = projectSelections
        ? _.sortBy(Object.keys(projectSelections).filter((project) => projectSelections[project]))
        : []


    // TODO: construct stacked area chart in d3
    // const svgRef = React.useRef<SVGSVGElement>()
    // React.useEffect(() => {
    //     if (!svgRef.current) return
    //     const svg = stack()
    //         .keys(selectedProjects)
    //         .value((d, key) => d[key])


    return <>
        {error && <Message negative>
            <h4>An error occurred while getting projects</h4>
            <p>{JSON.stringify(error)}</p>
        </Message>}
        {!!projectSelections && showProjectSelector && <Form>
            {Object.keys(projectSelections).map((project) =>
                <Form.Checkbox
                    key={`project-select-${project}`}
                    inline
                    label={project}
                    checked={projectSelections[project]}
                />
            )}
        </Form>}
        {!!projectSelections && !showProjectSelector && <Message info>
            <p>Showing {selectedProjects.length} projects from {start} to {end} ({selectedProjects.join(', ')})</p>
        </Message>}
        {isLoading && <LoadingDucks />}

        {propMap && <ResponsiveContainer width="95%" height={400}>
            <AreaChart height={500} width={700} data={propMap}>
                <CartesianGrid />
                <XAxis dataKey="date" />
                <YAxis domain={[0, 100]} name="percentage" />
                <Tooltip />
                {selectedProjects.map((project, idx) =>
                    <Area
                        key={`project-graph-area-${project}`}
                        dataKey={project}
                        stackId="1"
                        fill={chartColours[idx % chartColours.length]}
                        stroke={chartColours[idx % chartColours.length]}
                    />
                )}
                <Legend />
            </AreaChart>
        </ResponsiveContainer>}
    </>
}

export default SeqrProportionalMapGraph
