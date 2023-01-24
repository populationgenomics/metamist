import * as React from 'react'
import { tree, hierarchy } from 'd3'

interface PedigreeEntry {
    affected: number
    family_id: string
    individual_id: string
    maternal_id: string
    paternal_id: string
    sex: number
}

interface RenderPedigreeProps {
    data: PedigreeEntry[]
}

const connect = (d) => {
    return (
        'M' +
        (d.source.x - 30) +
        ',' +
        d.source.y +
        'H' +
        (d.source.x + 60) +
        'H' +
        d.source.x +
        'V' +
        (3 * d.source.y + 4 * d.target.y) / 7 +
        'H' +
        d.target.x +
        'V' +
        d.target.y
    )
}

const reshapeData = (data1: PedigreeEntry[]) => {
    const data: PedigreeEntry[] = [
        {
            family_id: '8600',
            individual_id: 'D21-0075',
            paternal_id: null,
            maternal_id: null,
            sex: 1,
            affected: 1,
        },
        {
            family_id: '8600',
            individual_id: 'D21-0074',
            paternal_id: '8600.1',
            maternal_id: '8600.2',
            sex: 2,
            affected: 1,
        },
        {
            family_id: '8600',
            individual_id: 'D21-0076',
            paternal_id: 'D21-0075',
            maternal_id: 'D21-0074',
            sex: 2,
            affected: 2,
        },
        {
            family_id: '8600',
            individual_id: 'D13-708',
            paternal_id: '8600.1',
            maternal_id: '8600.2',
            sex: 2,
            affected: 1,
        },
        {
            family_id: '8600',
            individual_id: '8600.1',
            paternal_id: null,
            maternal_id: null,
            sex: 1,
            affected: 1,
        },
        {
            family_id: '8600',
            individual_id: '8600.2',
            paternal_id: null,
            maternal_id: null,
            sex: 2,
            affected: 1,
        },
    ]

    if (data.length === 1) {
        const { individual_id, affected, sex } = data[0]
        return {
            name: individual_id,
            affected: affected,
            gender: sex,
            children: [],
        }
    }

    // const couples = Array.from(
    //     new Set(
    //         data
    //             .map((item) => `${item.paternal_id}+${item.maternal_id}`)
    //             .filter((item) => item !== "null+null")
    //     )
    // );
    // let newData = {};
    // let temp = [];
    // couples.forEach((item) => {
    //     const [dad, mum] = item.split("+");
    //     const siblings = data.filter(
    //         (i) => i.maternal_id === mum && i.paternal_id === dad
    //     );
    // if
    // newData = {name: item, children: siblings}
    // });

    const children = data.filter((item) => item.paternal_id && item.maternal_id)
    const collapsedChildren = children.reduce(
        (arr: { name?: string; children?: {}[] }[], item: PedigreeEntry) => {
            const exists = arr.findIndex(
                (i) => i.name === `${item.paternal_id}+${item.maternal_id}`
            )
            if (exists > -1) {
                arr[exists]['children']?.push({
                    name: item.individual_id,
                    affected: item.affected,
                    gender: item.sex,
                })
            } else {
                arr.push({
                    name: `${item.paternal_id}+${item.maternal_id}`,
                    children: [
                        {
                            name: item.individual_id,
                            affected: item.affected,
                            gender: item.sex,
                        },
                    ],
                })
            }
            return arr
        },
        []
    )

    return collapsedChildren[0]
}

export const RenderPedigree: React.FunctionComponent<RenderPedigreeProps> = ({ data }) => {
    const reshapedData = reshapeData(data)
    const margin = React.useMemo(() => ({ top: 100, right: 50, bottom: 100, left: 50 }), [])
    const width = 900 - margin.left - margin.right
    const height = 500 - margin.top - margin.bottom

    const ped = tree()
        .separation(function (a, b) {
            return a.parent === b.parent ? 1 : 1.2
        })
        .size([width, height])

    return (
        <>
            <svg
                width={width + margin.left + margin.right}
                height={height + margin.top + margin.bottom}
            >
                <g transform={`translate(${margin.left}, ${margin.top})`}>
                    {ped(hierarchy(reshapedData))
                        .links()
                        .map((item) => (
                            <path
                                fill={'none'}
                                stroke="black"
                                shapeRendering="crispEdges"
                                d={connect(item)}
                            />
                        ))}
                    {ped(hierarchy(reshapedData))
                        .descendants()
                        .map((item) => (
                            <g>
                                {item.data.name.includes('+') ? (
                                    <>
                                        <rect
                                            width={80}
                                            height={80}
                                            fill={
                                                data.find(
                                                    (i) =>
                                                        i.individual_id ===
                                                        item.data.name.split('+')[0]
                                                )?.affected === 1
                                                    ? 'black'
                                                    : 'white'
                                            }
                                            stroke="black"
                                            x={item.x - 110}
                                            y={item.y - 40}
                                        />
                                        <text
                                            fontSize={16}
                                            fill="black"
                                            x={item.x - 70}
                                            y={item.y + 60}
                                            style={{ textAnchor: 'middle' }}
                                        >
                                            {item.data.name.split('+')[0]}
                                        </text>
                                        <rect
                                            width={80}
                                            height={80}
                                            fill={
                                                data.find(
                                                    (i) =>
                                                        i.individual_id ===
                                                        item.data.name.split('+')[1]
                                                )?.affected === 1
                                                    ? 'black'
                                                    : 'white'
                                            }
                                            stroke="black"
                                            x={item.x + 30}
                                            y={item.y - 40}
                                        />
                                        <text
                                            fontSize={16}
                                            fill="black"
                                            x={item.x + 70}
                                            y={item.y + 60}
                                            style={{ textAnchor: 'middle' }}
                                        >
                                            {item.data.name.split('+')[1]}
                                        </text>
                                    </>
                                ) : (
                                    <>
                                        <rect
                                            width={80}
                                            height={80}
                                            fill={item.data.affected === 1 ? 'black' : 'white'}
                                            stroke="black"
                                            x={item.x - 40}
                                            y={item.y - 40}
                                        />

                                        <text
                                            fontSize={16}
                                            fill="black"
                                            x={item.x}
                                            y={item.y + 60}
                                            style={{ textAnchor: 'middle' }}
                                        >
                                            {item.data.name}
                                        </text>
                                    </>
                                )}
                            </g>
                        ))}
                </g>
            </svg>
        </>
    )
}
