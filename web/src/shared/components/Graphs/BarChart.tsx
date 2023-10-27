import { useEffect, useRef, useState } from 'react'
import { axisBottom, axisLeft, ScaleBand, scaleBand, ScaleLinear, scaleLinear, select } from 'd3'

export interface IData {
    label: string
    value: number
}

interface BarChartProps {
    data: IData[]
}

interface AxisBottomProps {
    scale: ScaleBand<string>
    transform: string
}

interface AxisLeftProps {
    scale: ScaleLinear<number, number, never>
}

interface BarsProps {
    data: BarChartProps['data']
    height: number
    scaleX: AxisBottomProps['scale']
    scaleY: AxisLeftProps['scale']
}

function AxisBottom({ scale, transform }: AxisBottomProps) {
    const ref = useRef<SVGGElement>(null)

    useEffect(() => {
        if (ref.current) {
            //   select(ref.current).call(axisBottom(scale));
            select(ref.current)
                .call(axisBottom(scale))
                .selectAll('text')
                .style('text-anchor', 'end')
                .attr('dx', '-0.8em')
                .attr('dy', '0.15em')
                .attr('transform', 'translate(-10, 0) rotate(-25)')
        }
    }, [scale])

    return <g ref={ref} transform={transform} />
}

function AxisLeft({ scale }: AxisLeftProps) {
    const ref = useRef<SVGGElement>(null)

    useEffect(() => {
        if (ref.current) {
            select(ref.current)
                .call(axisLeft(scale))
                .selectAll('text')
                .style('text-anchor', 'end')
                .attr('dx', '-0.8em')
                .attr('dy', '0.15em')
                .attr('transform', 'translate(-10, 0)')
        }
    }, [scale])

    return <g ref={ref} />
}

function Bars({ data, height, scaleX, scaleY }: BarsProps) {
    return (
        <>
            {data.map(({ value, label }) => (
                <rect
                    key={`bar-${label}`}
                    x={scaleX(label)}
                    y={scaleY(value)}
                    width={scaleX.bandwidth()}
                    height={height - scaleY(value)}
                    fill="darkblue"
                />
            ))}
        </>
    )
}

export function BarChart({ data }: BarChartProps) {
    const margin = { top: 10, right: 0, bottom: 150, left: 80 }
    //   const width = 1000 - margin.left - margin.right;
    const height = 400 - margin.top - margin.bottom
    const parentRef = useRef<HTMLDivElement>(null)
    const [width, setWidth] = useState(0)

    const scaleX = scaleBand()
        .domain(data.map(({ label }) => label))
        .range([0, width - margin.left - margin.right])
        .padding(0.5)
    const scaleY = scaleLinear()
        .domain([0, Math.max(...data.map(({ value }) => value))])
        .range([height, 0])

    useEffect(() => {
        setWidth(parentRef.current?.clientWidth || 768)
    }, [])

    return (
        <svg
            width={width + margin.left + margin.right}
            height={height + margin.top + margin.bottom}
        >
            <g transform={`translate(${margin.left}, ${margin.top})`}>
                <AxisBottom scale={scaleX} transform={`translate(0, ${height})`} />
                <AxisLeft scale={scaleY} />
                <Bars data={data} height={height} scaleX={scaleX} scaleY={scaleY} />
            </g>
        </svg>
    )
}
