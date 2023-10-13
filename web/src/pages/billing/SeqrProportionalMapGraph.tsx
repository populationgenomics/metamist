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
} from 'd3'

import LoadingDucks from '../../shared/components/LoadingDucks/LoadingDucks'
import { AnalysisApi, Project, ProjectApi, ProportionalDateTemporalMethod } from '../../sm-api'
import { Message, Select } from 'semantic-ui-react'

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
        React.useState<{ [m in ProportionalDateTemporalMethod]: IPropMapData[] }>()

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
                    [tMethod in ProportionalDateTemporalMethod]: IPropMapData[]
                } = Object.keys(summary.data).reduce(
                    (prev, tMethod) => ({
                        ...prev,
                        [tMethod]: summary.data[tMethod].map((obj: IProportionalDateModel) => ({
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
                        })),
                    }),
                    {} as { [tMethod in ProportionalDateTemporalMethod]: IPropMapData[] }
                )

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
    window.addEventListener('resize', updateWindowWidth)

    let graphComponent: React.ReactElement | undefined = undefined

    if (allPropMapData) {
        let data = allPropMapData[temporalMethod]
        // svg sizing info
        const margin = { top: 10, right: 240, bottom: 100, left: 80 }
        const width = graphWidth
        const minHeightForProjects = 25 + (Object.keys(projectSelections || {}).length + 1) * 20
        const height = Math.min(1200, Math.max(minHeightForProjects, Math.min(900, width * 0.6)))
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
            project: string,
            date: Date
        ) => {
            const tooltipDiv = tooltipRef.current
            const pos = pointer(event)
            if (tooltipDiv) {
                select(tooltipDiv).transition().duration(200).style('opacity', 0.9)
                select(tooltipDiv)
                    .html(
                        `<h4>${project} - ${date.getDate()}/${date.getMonth() + 1
                        }/${date.getFullYear()}</h4>
                        <h6>${(prevProp * 100).toFixed(1)}% &#8594; ${(newProp * 100).toFixed(
                            1
                        )}%</h6>
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

        graphComponent = (
            <>
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
                                        const date = areas[0].data.date
                                        const project = selectedProjects[i]
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
                                                        project,
                                                        date
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
                                // debugger
                                if (projectStart === -1) {
                                    return <React.Fragment key={`bigArea-${i}`}></React.Fragment>
                                }
                                return (
                                    <path
                                        key={`bigArea-${i}`}
                                        d={areaGenerator(
                                            area.slice(
                                                Math.max(0, projectStart - 1),
                                                area.length + 1
                                            )
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
                                x={`${innerWidth / 2}`}
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
                    <g transform={`translate(${width - margin.right + 30}, ${margin.top + 15})`}>
                        <text fontSize={20}>Projects</text>
                        {selectedProjects.map((project, i) => (
                            <React.Fragment key={`${project}-key`}>
                                <g
                                    transform={`translate(0, ${25 + i * 20})`}
                                    onMouseEnter={() => {
                                        setHovered(project)
                                    }}
                                    onMouseLeave={() => {
                                        setHovered('')
                                    }}
                                >
                                    <circle
                                        // cy={25 + i * 25}
                                        cx={10}
                                        r={8}
                                        fill={interpolateRainbow(i / selectedProjects.length)}
                                    />
                                    <text key={`${project}-legend`} x={30} y={5}>
                                        {project}
                                    </text>
                                </g>
                            </React.Fragment>
                        ))}
                    </g>
                </svg>
            </>
        )
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
