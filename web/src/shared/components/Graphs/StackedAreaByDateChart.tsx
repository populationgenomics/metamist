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
    TimeInterval,
    utcHour,
    stackOffsetNone,
} from 'd3'
import _ from 'lodash'
import React from 'react'
import { Message } from 'semantic-ui-react'

export interface IStackedAreaByDateChartData {
    date: Date
    values: { [key: string]: number }
}

interface IStackedAreaByDateChartProps {
    start?: Date
    end?: Date

    data?: IStackedAreaByDateChartData[]
    keys: string[]
    isPercentage: boolean
    xLabel: string
    yLabel: string
    seriesLabel: string
    extended?: boolean
    showDate?: boolean
}

function getDisplayValue(value: number, isPercentage: boolean) {
    if (isPercentage) {
        return `${(value * 100).toFixed(1)}%`
    }
    return `${value}`
}

function getTimeInterval(timeDiffMinutes: number) {
    if (timeDiffMinutes < 60 * 24) {
        // less than one day
        return utcHour.every(1)
    }
    if (timeDiffMinutes < 60 * 24 * 28) {
        // less than one month
        return utcDay.every(1)
    }
    if (timeDiffMinutes < 60 * 24 * 365) {
        // less than one year
        return utcMonth.every(1)
    }
    // greater than 1 year
    return utcMonth.every(3)
}

export const StackedAreaByDateChart: React.FC<IStackedAreaByDateChartProps> = ({
    data,
    keys,
    start,
    end,
    isPercentage,
    xLabel,
    yLabel,
    seriesLabel,
    extended,
    showDate,
}) => {
    if (!data || data.length === 0) {
        return <React.Fragment />
    }

    const tooltipRef = React.useRef()
    const containerDivRef = React.useRef<HTMLDivElement>()
    const [hoveredIndex, setHoveredIndex] = React.useState<number | null>(null)
    const [graphWidth, setGraphWidth] = React.useState<number>(768)

    const _start = start || _.min(data.map((d) => d.date))
    const _end = end || _.max(data.map((d) => d.date))

    React.useEffect(() => {
        function updateWindowWidth() {
            setGraphWidth(containerDivRef.current?.clientWidth || 768)
        }
        if (containerDivRef.current) {
            updateWindowWidth()
        }
        window.addEventListener('resize', updateWindowWidth)

        return () => {
            window.removeEventListener('resize', updateWindowWidth)
        }
    }, [])

    if (!_start || !_end) {
        return (
            <Message error>
                Start ({start}) / End ({end}) were not valid
            </Message>
        )
    }

    const margin = { top: 10, right: 240, bottom: 100, left: 80 }
    const width = graphWidth

    const minHeightForProjects = 25 + (keys.length + 1) * 20

    const height = Math.min(950, Math.max(minHeightForProjects, Math.min(500, width * 0.6)))
    const innerWidth = width - margin.left - margin.right
    const innerHeight = height - margin.top - margin.bottom
    const id = '1'

    // d3 function that turns the data into stacked proportions
    const stackedData = stack()
        .offset(extended ? stackOffsetExpand : stackOffsetNone)
        .keys(keys)(data.map((d) => ({ date: d.date, ...d.values })))

    // function for generating the x Axis
    // domain refers to the min and max of the data (in this case earliest and latest dates)
    // range refers to the min and max pixel positions on the screen
    // basically it is a mapping of pixel positions to data values
    const xScale = scaleTime()
        .domain(extent(data, (d) => d.date)) // date is a string, will this take a date object? Yes :)
        .range([0, width - margin.left - margin.right])

    // function for generating the y Axis
    // no domain needed as it defaults to [0, 1] which is appropriate for proportions
    const maxY = data.map((d: IStackedAreaByDateChartData) =>
        Object.values(d.values).reduce((acc, val) => acc + val, 0)
    )

    const yScale = extended
        ? scaleLinear().range([height - margin.top - margin.bottom, 0])
        : scaleLinear()
            .domain([0, Math.max(...maxY.flatMap((val) => val))])
            .range([height - margin.top - margin.bottom, 0])

    // function that assigns each category a colour
    // can fiddle with the schemeAccent parameter for different colour scales - see https://d3js.org/d3-scale-chromatic/categorical#schemeAccent
    // const colour = scaleOrdinal().domain(selectedProjects).range(schemeSet3)

    // function that takes the various stacked data info and generates an svg path element (magically)
    const areaGenerator = area()
        .x((d, idx) => xScale(d.data.date))
        .y0((d) => yScale(d[0]))
        .y1((d) => yScale(d[1]))

    const diffMinutes = Math.round((_end?.valueOf() - _start?.valueOf()) / 60000)
    let interval: TimeInterval | null = getTimeInterval(diffMinutes)

    const mouseover = (
        event: React.MouseEvent<SVGPathElement, MouseEvent>,
        prevValue: number,
        newValue: number,
        key: string,
        date: Date
    ) => {
        const tooltipDiv = tooltipRef.current
        const pos = pointer(event)
        if (!tooltipDiv) {
            return
        }

        select(tooltipDiv).transition().duration(200).style('opacity', 0.9)
        select(tooltipDiv)
            .html(
                `<h4>
                    ${key} - ${date.getDate()}/
                    ${date.getMonth() + 1}/
                    ${date.getFullYear()}
                </h4>
                <h6>
                    ${getDisplayValue(prevValue, isPercentage)}
                    &#8594;
                    ${getDisplayValue(newValue, isPercentage)}
                </h6>
                `
            )
            .style('left', `${pos[0] + 95}px`)
            .style('top', `${pos[1] + 100}px`)
    }

    const mouseout = () => {
        const tooltipDiv = tooltipRef.current
        if (tooltipDiv) {
            select(tooltipDiv).transition().duration(500).style('opacity', 0)
        }
    }

    return (
        <div ref={containerDivRef}>
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
                                    {/* change this for different date formats */}
                                    {showDate
                                        ? `${tick.toLocaleString('en-us', {
                                            day: 'numeric',
                                            month: 'short',
                                            year: 'numeric',
                                        })}`
                                        : `${tick.toLocaleString('en-us', {
                                            month: 'short',
                                            year: 'numeric',
                                        })}`}
                                </text>
                                {/* this is the tiny vertical tick line that getting drawn (6 pixels tall) */}
                                <line y2={6} stroke="black" />
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
                                    {getDisplayValue(tick, isPercentage)}
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
                                        return <React.Fragment key={`${i}-${j}`}></React.Fragment>
                                    }
                                    const areas = area.slice(j, j + 2)
                                    // don't draw empty areas
                                    if (
                                        areas[0][1] - areas[0][0] === 0 &&
                                        areas[1][1] - areas[1][0] === 0
                                    ) {
                                        return <React.Fragment key={`${i}-${j}`}></React.Fragment>
                                    }

                                    const colour = interpolateRainbow(i / keys.length)
                                    // @ts-ignore
                                    const key = keys[i]
                                    const date = data[j]?.date
                                    // const colour = colors[i]
                                    const kwargs: React.SVGProps<SVGPathElement> = {}
                                    if (date) {
                                        kwargs.onMouseEnter = (e) => {
                                            select(e.currentTarget).style('opacity', 0.6)
                                        }
                                        kwargs.onMouseMove = (e) => {
                                            mouseover(
                                                e,
                                                areas[0][1] - areas[0][0],
                                                areas[1][1] - areas[1][0],
                                                key,
                                                date
                                            )
                                        }

                                        kwargs.onMouseLeave = (e) => {
                                            select(e.currentTarget).style('opacity', 1)
                                            mouseout()
                                        }
                                    }
                                    return (
                                        <path
                                            key={`${i}-${j}`}
                                            d={areaGenerator(areas)}
                                            style={{
                                                fill: colour,
                                                stroke: colour,
                                            }}
                                            {...kwargs}
                                        />
                                    )
                                })}
                            </React.Fragment>
                        ))}

                        {stackedData.map((area, i) => {
                            const areaStart = area.findIndex((p) => p[1] - p[0])
                            // debugger
                            if (areaStart === -1) {
                                return <React.Fragment key={`bigArea-${i}`}></React.Fragment>
                            }
                            return (
                                <path
                                    key={`bigArea-${i}`}
                                    d={areaGenerator(
                                        area.slice(
                                            // clamp at the start to avoid unintentional wraparound
                                            Math.max(0, areaStart - 1),
                                            area.length + 1
                                        )
                                    )}
                                    style={{
                                        stroke: i === hoveredIndex ? 'black' : 'none',
                                        strokeWidth: i === hoveredIndex ? 2 : 'none',
                                        opacity: i === hoveredIndex ? 1 : 0,
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
                            {xLabel}
                        </text>
                    </g>

                    {/* y-axis label */}
                    <g
                        id={`${id}-y-axis-label`}
                        transform={`rotate(-90) translate(-${innerHeight / 2}, -60)`}
                    >
                        <text textAnchor="middle" fontSize={20}>
                            {yLabel}
                        </text>
                    </g>
                </g>
                <g transform={`translate(${width - margin.right + 30}, ${margin.top + 15})`}>
                    <text fontSize={20}>{seriesLabel}</text>
                    {keys.map((project, i) => (
                        <React.Fragment key={`${project}-key`}>
                            <g
                                transform={`translate(0, ${25 + i * 20})`}
                                onMouseEnter={() => {
                                    setHoveredIndex(i)
                                }}
                                onMouseLeave={() => {
                                    setHoveredIndex(null)
                                }}
                            >
                                <circle
                                    // cy={25 + i * 25}
                                    cx={10}
                                    r={8}
                                    fill={interpolateRainbow(i / keys.length)}
                                />
                                <text key={`${project}-legend`} x={30} y={5}>
                                    {project}
                                </text>
                            </g>
                        </React.Fragment>
                    ))}
                </g>
            </svg>
        </div>
    )
}
