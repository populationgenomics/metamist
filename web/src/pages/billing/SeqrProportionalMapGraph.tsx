import * as React from 'react'
import _ from 'lodash'

import { Container, Form, Message } from 'semantic-ui-react'

import {
    scaleLinear,
    extent,
    scaleOrdinal,
    stack,
    csv,
    area,
    stackOffsetExpand,
    schemeAccent,
    scaleTime,
    utcDay,
    timeFormat,
} from 'd3'

import LoadingDucks from '../../shared/components/LoadingDucks/LoadingDucks'
import { AnalysisApi, Project, ProjectApi } from '../../sm-api'

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

interface IPropMapData {
    date: Date
    [project: string]: number
}

const SeqrProportionalMapGraph: React.FunctionComponent<ISeqrProportionalMapGraphProps> = ({
    start,
    end,
}) => {
    const [isLoading, setIsLoading] = React.useState<boolean>(true)
    const [error, setError] = React.useState<string | undefined>()
    const [showProjectSelector, setShowProjectSelector] = React.useState<boolean>(false)
    const [sequencingType, setSequencingType] = React.useState<string>('genome')
    const [projectSelections, setProjectSelections] = React.useState<
        { [key: string]: boolean } | undefined
    >()
    const [data, setData] = React.useState<IPropMapData[]>()
    // const [propMap, setPropMap] = React.useState<>()

    function updateProjects(projects: { [key: string]: boolean }) {
        const updatedProjects = { ...(projectSelections || {}), ...projects }
        setProjectSelections(updatedProjects)
        loadPropMap(updatedProjects)
    }

    function loadPropMap(projects: { [key: string]: boolean } = {}) {
        const projectsToSearch = Object.keys(projects).filter((project) => projects[project])

        const api = new AnalysisApi()
        const defaultPropMap = projectsToSearch.reduce((prev, p) => ({ ...prev, [p]: 0 }), {})
        api.getProportionateMap(sequencingType, projectsToSearch, start, end)
            .then((summary) => {
                setIsLoading(false)

                const graphData: IPropMapData[] = summary.data.map(
                    (obj: IProportionalDateModel) => ({
                        date: obj.date,
                        ...defaultPropMap,
                        ...obj.projects.reduce(
                            (prev: { [project: string]: number }, projectObj) => ({
                                ...prev,
                                // in percentage, rounded to 2 decimal places
                                [projectObj.project]: projectObj.percentage,
                            }),
                            {}
                        ),
                    })
                )
                const projectsToSee = new Set(projectsToSearch)
                for (let index = 1; index < graphData.length; index++) {
                    const graphObj = graphData[index]
                    if (projectsToSearch.length == 0) continue
                    // make sure the entry BEFORE a project is visible,
                    // it's set to 0 to make the graph prettier
                    for (const project of projectsToSee) {
                        if (project in graphObj) {
                            projectsToSee.delete(project)
                            graphData[index - 1][project] = 0
                        }
                        if (projectsToSee.size == 0) break
                    }
                }
                setData(graphData)
            })
            .catch((er) => {
                setError(er.message)
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

    // TODO, uncomment later
    // React.useEffect(() => {
    //     getSeqrProjects()
    // }, [])

    React.useEffect(() => {
        setProjectSelections(
            ['project1', 'project2', 'project3'].reduce(
                (prev: { [project: string]: boolean }, project: string) => ({
                    ...prev,
                    [project]: true,
                }),
                {}
            )
        )

        setData([
            {
                date: new Date('2021-01-01'), // need to be date objects
                project1: 0.1,
                project2: 0.2,
                project3: 0.7,
            },
            {
                date: new Date('2021-01-02'),
                project1: 0.2,
                project2: 0.3,
                project3: 0.5,
            },
            {
                date: new Date('2021-01-03'),
                project1: 0.3,
                project2: 0.4,
                project3: 0.3,
            },
            {
                date: new Date('2021-01-04'),
                project1: 0, // 0 works fine :)
                project2: 0.8,
                project3: 0.2,
            },
            {
                date: new Date('2021-01-05'),
                project1: 10, // proportions don't need to be precomputed if that's easier, the stackedData call below can do  it
                project2: 5, // can just use regular values
                project3: 20,
            },
        ])
    }, [])

    if (!data) {
        return 'Loading'
    }

    const selectedProjects: string[] = projectSelections
        ? _.sortBy(Object.keys(projectSelections).filter((project) => projectSelections[project]))
        : []

    // svg sizing info
    const margin = { top: 10, right: 30, bottom: 100, left: 80 }
    const width = 1500 - margin.left - margin.right
    const height = 1000 - margin.top - margin.bottom
    const id = '1'

    // d3 function that turns the data into stacked proportions
    const stackedData = stack().offset(stackOffsetExpand).keys(selectedProjects)(data)

    // function for generating the x Axis
    // domain refers to the min and max of the data (in this case earliest and latest dates)
    // range refers to the min and max pixel positions on the screen
    // basically it is a mapping of pixel positions to data values
    const xScale = scaleTime()
        .domain(extent(data, (d) => d.date)) // date is a string, will this take a date object? Yes :)
        .range([0, width - margin.left - margin.right])
    // .tickFormat(timeFormat('%B %d, %Y'))

    // function for generating the y Axis
    // no domain needed as it defaults to [0, 1] which is appropriate for proportions
    const yScale = scaleLinear().range([height - margin.top - margin.bottom, 0])

    // function that assigns each category a colour
    // can fiddle with the schemeAccent parameter for different colour scales - see https://d3js.org/d3-scale-chromatic/categorical#schemeAccent
    const color = scaleOrdinal(schemeAccent).domain(selectedProjects)

    // function that takes the various stacked data info and generates an svg path element (magically)
    const areaGenerator = area()
        .x((d) => xScale(d.data.date))
        .y0((d) => yScale(d[0]))
        .y1((d) => yScale(d[1]))

    return (
        data && (
            <svg id={id} width={width} height={height}>
                {/* transform and translate move the relative (0,0) so you can draw accurately. Consider svg as a cartesian plane with (0, 0) top left and positive directions left and down the page
                then to draw in svg you just need to give coordinates. We've specified the width and height above so this svg 'canvas' can be drawn on anywhere between pixel 0 and the max height and width pixels */}
                <g transform={`translate(${margin.left}, ${margin.top})`}>
                    {/* x-axis */}
                    <g id={`${id}-x-axis`}>
                        {/* draws the main x axis line */}
                        <line
                            y1={`${height - margin.top - margin.bottom}`}
                            y2={`${height - margin.top - margin.bottom}`}
                            x2={`${width}`}
                            stroke="black"
                        />
                        {/* draws the little ticks marks off the x axis + labels 
                            xScale.ticks() generates a list of evenly spaces ticks from min to max domain
                            You can pass an argument to ticks() to specify number of ticks to generate 
                            Calling xScale(tick) turns a tick value into a pixel position to be drawn 
                            eg in the domain [2000, 2010] and range[0, 200] passing 2005 would be 50% of the way across the domain so 50% of the way between min and max specified pixel positions so it would draw at 100
                            */}
                        {xScale.ticks(utcDay.every(1)).map(
                            (
                                tick // can change this to set how many ticks you want
                            ) => (
                                <g
                                    key={tick}
                                    transform={`translate(${xScale(tick)}, ${
                                        height - margin.top - margin.bottom
                                    })`}
                                >
                                    <text
                                        y={8}
                                        transform="translate(0, 10)rotate(-45)"
                                        textAnchor="end"
                                        alignmentBaseline="middle"
                                        fontSize={14}
                                        cursor="help"
                                    >
                                        {tick.toDateString()}
                                        {/* change this for different date formats */}
                                    </text>
                                    <line y2={6} stroke="black" />{' '}
                                    {/* this is the tiny vertical tick line that getting drawn (6 pixels tall) */}
                                </g>
                            )
                        )}
                    </g>

                    {/* y-axis (same as above) */}
                    <g id={`${id}-y-axis`}>
                        <line y2={`${height - margin.top - margin.bottom}`} stroke="black" />
                        {yScale.ticks().map((tick) => (
                            <g key={tick} transform={`translate(0, ${yScale(tick)})`}>
                                <text
                                    key={tick}
                                    textAnchor="end"
                                    alignmentBaseline="middle"
                                    fontSize={14}
                                    fontWeight={600}
                                    x={-8}
                                    y={3}
                                >
                                    {tick}
                                </text>
                                <line x2={-3} stroke="black" />
                                <line
                                    x2={`${width - margin.left - margin.right}`}
                                    stroke="lightgrey"
                                />
                            </g>
                        ))}
                    </g>

                    {/* stacked areas */}
                    <g id={`${id}-stacked-areas`}>
                        {/* for each 'project', draws a path (using path function) and fills it a new colour (using colour function) */}
                        {stackedData.map((area, i) => (
                            <path
                                key={i}
                                d={areaGenerator(area)}
                                style={{ fill: color(selectedProjects[i]) }}
                            />
                        ))}
                    </g>
                </g>
            </svg>
        )
    )
}

export default SeqrProportionalMapGraph
