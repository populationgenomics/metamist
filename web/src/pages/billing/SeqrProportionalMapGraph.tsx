import * as React from 'react'
import _ from 'lodash'

import {
    scaleLinear,
    extent,
    stack,
    area,
    stackOffsetExpand,
    scaleTime,
    utcDay,
    utcMonth,
    select,
    pointer,
    interpolateRainbow,
    selectAll,
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
    const tooltipRef = React.useRef()
    const [hovered, setHovered] = React.useState('')
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
                        date: new Date(obj.date),
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
            .catch((er: Error) => {
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

    React.useEffect(() => {
        getSeqrProjects()
    }, [])

    if (isLoading) {
        return <LoadingDucks />
    }

    if (!data) {
        return <p><em>No data</em></p>
    }

    const selectedProjects: string[] = projectSelections
        ? _.sortBy(Object.keys(projectSelections).filter((project) => projectSelections[project]))
        : []

    // svg sizing info
    const margin = { top: 10, right: 400, bottom: 100, left: 80 }
    const height = 1000
    const width = 2400
    const innerWidth = width - margin.left - margin.right
    const innerHeight = height - margin.top - margin.bottom
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

    // function for generating the y Axis
    // no domain needed as it defaults to [0, 1] which is appropriate for proportions
    const yScale = scaleLinear().range([height - margin.top - margin.bottom, 0])

    // function that assigns each category a colour
    // can fiddle with the schemeAccent parameter for different colour scales - see https://d3js.org/d3-scale-chromatic/categorical#schemeAccent
    // const colour = scaleOrdinal().domain(selectedProjects).range(schemeSet3)

    // function that takes the various stacked data info and generates an svg path element (magically)
    const areaGenerator = area()
        .x((d) => xScale(d.data.date))
        .y0((d) => yScale(d[0]))
        .y1((d) => yScale(d[1]))

    let interval = utcDay.every(10)
    // more than 3 months
    if (new Date(end).valueOf() - new Date(start).valueOf() > 1000 * 60 * 60 * 24 * 90) {
        interval = utcMonth.every(1)
    }

    const mouseover = (
        event: React.MouseEvent<SVGPathElement, MouseEvent>,
        prevProp: number,
        newProp: number,
        project: string
    ) => {
        const tooltipDiv = tooltipRef.current
        const pos = pointer(event)
        if (tooltipDiv) {
            select(tooltipDiv).transition().duration(200).style('opacity', 0.9)
            select(tooltipDiv)
                .html(
                    `<h4>${project}</h4><h6>${(prevProp * 100).toFixed(1)}% &#8594; ${(
                        newProp * 100
                    ).toFixed(1)}%</h6>
                `
                )
                .style('left', `${pos[0] + 95}px`)
                .style('top', `${pos[1] + 100}px`)
        }
    }

    const mouseout = () => {
        const tooltipDiv = tooltipRef.current
        if (tooltipDiv) {
            select(tooltipDiv).transition().duration(500).style('opacity', 0)
        }
    }

    return <>
        <h5>Seqr proportional costs</h5>
        <div
            className="tooltip"
            ref={tooltipRef}
            style={{
                position: 'absolute',
                textAlign: 'center',
                padding: '2px',
                font: '12px sans-serif',
                background: 'lightsteelblue',
                border: '0px',
                borderRadius: '8px',
                pointerEvents: 'none',
                opacity: 0,
            }}
        />
        <svg id={id} width={width} height={height}>
            {/* transform and translate move the relative (0,0) so you can draw accurately. Consider svg as a cartesian plane with (0, 0) top left and positive directions left and down the page
                then to draw in svg you just need to give coordinates. We've specified the width and height above so this svg 'canvas' can be drawn on anywhere between pixel 0 and the max height and width pixels */}
            <g transform={`translate(${margin.left}, ${margin.top})`}>
                {/* x-axis */}
                <g id={`${id}-x-axis`}>
                    {/* draws the little ticks marks off the x axis + labels 
                            xScale.ticks() generates a list of evenly spaces ticks from min to max domain
                            You can pass an argument to ticks() to specify number of ticks to generate 
                            Calling xScale(tick) turns a tick value into a pixel position to be drawn 
                            eg in the domain [2000, 2010] and range[0, 200] passing 2005 would be 50% of the way across the domain so 50% of the way between min and max specified pixel positions so it would draw at 100
                            */}
                    {xScale.ticks(interval).map((tick) => (
                        <g
                            key={`x-tick-${tick.toString()}`}
                            transform={`translate(${xScale(tick)}, ${innerHeight})`}
                        >
                            <text
                                y={8}
                                transform="translate(0, 10)rotate(-45)"
                                textAnchor="end"
                                alignmentBaseline="middle"
                                fontSize={14}
                                cursor="help"
                            >
                                {`${tick.toLocaleString('en-us', {
                                    month: 'short',
                                    year: 'numeric',
                                })}`}
                                {/* change this for different date formats */}
                            </text>
                            <line y2={6} stroke="black" />
                            {/* this is the tiny vertical tick line that getting drawn (6 pixels tall) */}
                        </g>
                    ))}
                </g>

                {/* y-axis (same as above) */}
                <g id={`${id}-y-axis`}>
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
                                {tick * 100}%
                            </text>
                            <line x2={-3} stroke="black" />
                        </g>
                    ))}
                </g>

                {/* stacked areas */}
                <g id={`${id}-stacked-areas`}>
                    {/* for each 'project', draws a path (using path function) and fills it a new colour (using colour function) */}
                    {stackedData.map((area, i) => (
                        <React.Fragment key={`bigArea-${i}`}>
                            {area.map((region, j) => {
                                // don't draw an extra area at the end
                                if (j + 1 >= area.length) {
                                    return (
                                        <React.Fragment key={`${i}-${j}`}></React.Fragment>
                                    )
                                }
                                const areas = area.slice(j, j + 2)
                                // don't draw empty areas
                                if (
                                    areas[0][1] - areas[0][0] === 0 &&
                                    areas[1][1] - areas[1][0] === 0
                                ) {
                                    return (
                                        <React.Fragment key={`${i}-${j}`}></React.Fragment>
                                    )
                                }

                                const colour = interpolateRainbow(
                                    i / selectedProjects.length
                                )
                                // const colour = colors[i]
                                return (
                                    <path
                                        key={`${i}-${j}`}
                                        d={areaGenerator(areas)}
                                        style={{
                                            fill: colour,
                                            stroke: colour,
                                        }}
                                        onMouseEnter={(e) => {
                                            select(e.target).style('opacity', 0.6)
                                        }}
                                        onMouseMove={(e) =>
                                            mouseover(
                                                e,
                                                areas[0][1] - areas[0][0],
                                                areas[1][1] - areas[1][0],
                                                selectedProjects[i]
                                            )
                                        }
                                        onMouseLeave={(e) => {
                                            select(e.target).style('opacity', 1)
                                            mouseout()
                                        }}
                                    />
                                )
                            })}
                        </React.Fragment>
                    ))}

                    {stackedData.map((area, i) => {
                        const projectStart = area.findIndex((p) => p[1] - p[0])
                        if (projectStart === -1) {
                            return <React.Fragment key={`bigArea-${i}`}></React.Fragment>
                        }
                        return (
                            <path
                                key={`bigArea-${i}`}
                                d={areaGenerator(
                                    area.slice(projectStart - 1, area.length + 1)
                                )}
                                style={{
                                    stroke:
                                        selectedProjects[i] === hovered ? 'black' : 'none',
                                    strokeWidth:
                                        selectedProjects[i] === hovered ? 2 : 'none',
                                    opacity: selectedProjects[i] === hovered ? 1 : 0,
                                    fill: 'none',
                                }}
                            />
                        )
                    })}
                </g>

                {/* draws the main x axis line */}
                <line
                    y1={`${innerHeight}`}
                    y2={`${innerHeight}`}
                    x2={`${innerWidth}`}
                    stroke="black"
                />

                {/* draws the main y axis line */}
                <line y2={`${innerHeight}`} stroke="black" />

                {/* x-axis label */}
                <g id={`${id}-x-axis-label`}>
                    <text
                        x={innerWidth / 2}
                        y={innerHeight + 80}
                        fontSize={20}
                        textAnchor="middle"
                    >
                        {'Date'}
                    </text>
                </g>

                {/* y-axis label */}
                <g
                    id={`${id}-y-axis-label`}
                    transform={`rotate(-90) translate(-${innerHeight / 2}, -60)`}
                >
                    <text textAnchor="middle" fontSize={20}>
                        {'Proportion'}
                    </text>
                </g>
            </g>
            <g transform={`translate(${width - margin.right + 30}, ${margin.top + 30})`}>
                <text fontSize={25}>Projects</text>
                {selectedProjects.map((project, i) => (
                    <React.Fragment key={`${project}-key`}>
                        <circle
                            cy={25 + i * 25}
                            cx={10}
                            r={10}
                            fill={interpolateRainbow(i / selectedProjects.length)}
                            onMouseEnter={() => {
                                setHovered(project)
                            }}
                            onMouseLeave={() => {
                                setHovered('')
                            }}
                        />
                        <text key={`${project}-legend`} y={30 + i * 25} x={30}>
                            {project}
                        </text>
                    </React.Fragment>
                ))}
            </g>
        </svg>
    </>
}

export default SeqrProportionalMapGraph
