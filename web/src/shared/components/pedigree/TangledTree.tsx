/* Inspired by https://observablehq.com/@nitaku/tangled-tree-visualization-ii */

/* eslint-disable no-param-reassign */
import * as React from 'react'
import _ from 'lodash'
import { min, max, sum, mean, extent } from 'd3'
import LoadingDucks from '../LoadingDucks/LoadingDucks'
import MuckError from '../MuckError'

interface PedigreeEntry {
    affected: number
    family_id: string
    individual_id: string
    maternal_id: string
    paternal_id: string
    sex: number
}

interface ModifiedPedEntry {
    affected: number
    family_id: string
    individual_id: string
    maternal_id: string
    paternal_id: string
    sex: number
    children: string[]
}

interface Node {
    id: string
    x: number
    y: number
    parents: Node[]
    parentsList: string[]
    level: number
    bundle?: Node
    bundles: Node[][]
    i?: number
    bundles_index?: { [key: string]: Node[] }
    span?: number
    height?: number
    links: Link[]
}

interface NodeList {
    nodes: Node[]
    bundles: Node[]
}

interface Link {
    source: Node
    target: Node
    xb: number
    xs: number
    xt: number
    yb: number
    ys: number
    yt: number
    x1: number
    y1: number
    x2: number
    y2: number
    bundle?: Node
}

// layout
const yPadding = 50
const xPadding = 50
const node_height = 22
const node_width = 70
const bundle_width = 14
const level_y_padding = 100
const horizontal_spacing = 100

const constructTangleLayout = (levels: NodeList[]) => {
    const nodes_index: Record<string, any> = {}
    // precompute level depth
    levels.forEach((l, i) =>
        l.nodes.forEach((n) => {
            n.level = i
            nodes_index[n.id] = n
        })
    )
    const nodes: Node[] = levels.map((l) => l.nodes).flat()
    nodes.forEach((d) => {
        d.parents = (d.parentsList === undefined ? [] : d.parentsList).map((p) => nodes_index[p])
    })

    // precompute bundles
    levels.forEach((l, i) => {
        const index: Record<string, any> = {}
        l.nodes.forEach((n) => {
            if (!n.parents || n.parents.length === 0) {
                return
            }
            const id = n.parents
                .map((d) => d.id)
                .sort()
                .join('-X-')
            if (id in index) {
                index[id].parents = index[id].parents.concat(n.parents)
            } else {
                index[id] = {
                    id,
                    parents: n.parents.slice(),
                    level: i,
                    span: i - (min(n.parents, (p) => p.level) ?? 0),
                }
            }
            n.bundle = index[id]
        })
        l.bundles = Object.keys(index).map((k) => index[k])
        l.bundles.forEach((b, j) => {
            b.i = j
        })
    })

    const links: Link[] = []
    nodes.forEach((d) => {
        if (d.parents) {
            d.parents.forEach((p) =>
                links.push({
                    source: d,
                    bundle: d.bundle,
                    target: p,
                    xt: p.x,
                    yt: p.y,
                    xb: d.bundle?.x ?? p.x,
                    yb: d.bundle?.y ?? p.y,
                    x1: d.bundle?.x ?? p.x,
                    y1: d.y - level_y_padding / 2,
                    x2: d.x,
                    y2: d.y - level_y_padding / 2,
                    xs: d.x,
                    ys: d.y,
                })
            )
        }
    })

    const bundles = levels.map((l) => l.bundles).flat()

    // reverse pointer from parent to bundles
    bundles.forEach((b) => {
        if (b && b.parents) {
            b.parents.forEach((p) => {
                if (p.bundles_index === undefined) {
                    p.bundles_index = {}
                }
                if (!(b.id in p.bundles_index)) {
                    p.bundles_index[b.id] = []
                }
                p.bundles_index[b.id].push(b)
            })
        }
    })

    nodes.forEach((n) => {
        if (n.bundles_index !== undefined) {
            n.bundles = Object.values(n.bundles_index)
        } else {
            n.bundles_index = {}
            n.bundles = []
        }
    })

    links.forEach((l) => {
        if (!l.bundle) {
            return
        }
        if (l.bundle.links === undefined) {
            l.bundle.links = []
        }
        l.bundle.links.push(l)
    })

    nodes.forEach((n) => {
        n.height = Math.max(1, n.bundles.length) - 1
    })

    let x_offset = xPadding
    let y_offset = yPadding
    levels.forEach((l) => {
        x_offset = xPadding
        l.nodes.forEach((n) => {
            n.x = x_offset
            n.y = y_offset

            x_offset += horizontal_spacing
        })
        y_offset += level_y_padding
    })

    const rebalanceNodes = () => {
        let moved = false
        const movedBundles: string[] = []
        const movedChildren: string[] = []
        levels.forEach((level) => {
            level.nodes.forEach((currentNode, movedIndex) => {
                const oldLevel = [...level.nodes]
                level.nodes.sort(
                    (a, b) =>
                        a.x - b.x ||
                        oldLevel.findIndex((c) => a.id === c.id) -
                        oldLevel.findIndex((c) => b.id === c.id)
                )
                if (movedIndex < level.nodes.length - 1) {
                    // not the last node
                    const nextNode = level.nodes[movedIndex + 1]
                    if (Math.abs(currentNode.x - nextNode.x) < horizontal_spacing) {
                        moved = true
                        const oldX = nextNode.x
                        nextNode.x = currentNode.x + horizontal_spacing
                        const movedX = nextNode.x - oldX
                        nextNode.bundles.forEach((bundle) => {
                            bundle
                                .filter((b) => !movedBundles.includes(b.id))
                                .forEach((b) => {
                                    movedBundles.push(b.id)
                                    b.links.forEach((l) => {
                                        l.xs += movedX
                                        l.xb += movedX
                                        if (movedChildren.includes(l.source.id)) {
                                            return
                                        }
                                        nodes_index[l.source.id].x += movedX
                                        movedChildren.push(l.source.id)
                                        l.x1 += movedX
                                        l.x2 += movedX
                                        // l.xt += movedX;
                                    })
                                })
                        })
                    }
                }
            })
        })
        return moved
    }

    levels.reverse().forEach((l) => {
        const seenBundles: string[] = []
        l.nodes.forEach((node) => {
            if (!node.bundle || seenBundles.includes(node.bundle.id)) {
                return
            }

            const minXParent = min(node.parents.map((p) => p.x)) || 0
            if (minXParent < node.x) {
                const amountToMove = node.x - minXParent
                node.parents.forEach((p) => {
                    p.x += amountToMove
                })
                const maxParent = node.parents.reduce((prev, current) =>
                    prev.x > current.x ? prev : current
                )
                // move rest of level accordingly
                const currentLevel = maxParent.level
                const movedIndex = nodes.findIndex((b) => b.id === maxParent.id)
                nodes.forEach((n, i) => {
                    if (n.level === currentLevel && i > movedIndex) {
                        n.x += amountToMove
                    }
                })
            }
            seenBundles.push(node.bundle.id)
        })
    })

    levels.forEach((l) => {
        l.bundles.forEach((b) => {
            b.x = sum(b.parents, (d) => d.x) / b.parents.length
            b.y = sum(b.parents, (d) => d.y) / b.parents.length
        })
    })

    links.forEach((l) => {
        l.xt = l.target.x
        l.yt = l.target.y
        l.xb = l.bundle?.x ?? l.target.x
        l.yb = l.bundle?.y ?? l.target.y
        l.x1 = l.bundle?.x ?? l.target.x
        l.y1 = l.source.y - level_y_padding / 2
        l.x2 = l.source.x
        l.y2 = l.source.y - level_y_padding / 2
        l.xs = l.source.x
        l.ys = l.source.y
    })

    // try centre parents
    let moved = true
    let numLoops = 0
    while (moved && numLoops < 1000) {
        moved = false
        /* eslint-disable @typescript-eslint/no-loop-func */
        levels.reverse().forEach((l) => {
            l.bundles.forEach((b) => {
                const avgX = mean(extent(b.links.map((li) => li.source.x))) || 0
                b.links.forEach((p) => {
                    if (p.x1 !== avgX) {
                        const oldxb = p.xb
                        p.xb = avgX
                        p.xt += avgX - oldxb
                        nodes_index[p.target.id].x = p.xt
                        p.x1 = avgX
                        moved = true
                    }
                })
            })
        })
        /* eslint-enable @typescript-eslint/no-loop-func */
        moved = moved || rebalanceNodes()
        links.forEach((l) => {
            l.xt = l.target.x
            l.yt = l.target.y
            l.x2 = l.source.x
            l.xs = l.source.x
            l.ys = l.source.y
        })
        if (!moved) {
            break
        }
        numLoops += 1
    }

    if (numLoops === 1000) {
        return { error: 'Infinite loop - could not generate pedigree' }
    }

    const minWidth = min(nodes, (n) => n.x) ?? 0
    const maxWidth = max(nodes, (n) => n.x) ?? 0
    const minHeight = min(nodes, (n) => n.y) ?? 0
    const maxHeight = max(nodes, (n) => n.y) ?? 0
    const layout = {
        width_dimensions: [minWidth - node_width, maxWidth + node_width + 2 * xPadding],
        height_dimensions: [
            minHeight - node_height / 2 - yPadding,
            maxHeight + node_height / 2 + 2 * yPadding,
        ],
        node_height,
        node_width,
        bundle_width,
        level_y_padding,
    }

    return { levels, nodes, nodes_index, links, bundles, layout }
}

const renderChart = (
    data: NodeList[],
    originalData: { [name: string]: PedigreeEntry },
    click: (e: string) => void
) => {
    const tangleLayout = constructTangleLayout(_.cloneDeep(data))

    if ('error' in tangleLayout) {
        return <MuckError message={`Ah Muck, couldn't resolve this pedigree`} />
    }

    return (
        <svg
            width={`${tangleLayout.layout.width_dimensions[1] - tangleLayout.layout.width_dimensions[0]
                }`}
            height={`${tangleLayout.layout.height_dimensions[1] - tangleLayout.layout.height_dimensions[0]
                }`}
            viewBox={`${tangleLayout.layout.width_dimensions[0]} ${tangleLayout.layout.height_dimensions[0]} ${tangleLayout.layout.width_dimensions[1]} ${tangleLayout.layout.height_dimensions[1]}`}
        >
            {tangleLayout.bundles.map((b) => {
                const d = b.links
                    .map(
                        (l) => `
                        M ${l.xt} ${l.yt}
                        L ${l.xb} ${l.yb}
                        L ${l.x1} ${l.y1}
                        L ${l.x2} ${l.y2}
                        L ${l.xs} ${l.ys}`
                    )
                    .join('')
                return (
                    <React.Fragment key={`${b.id}-bundle`}>
                        <path fill="none" d={`${d}`} stroke={'white'} strokeWidth="5" />
                        <path d={`${d}`} fill="none" stroke={'black'} strokeWidth="2" />
                    </React.Fragment>
                )
            })}

            {tangleLayout.nodes.map((n) => (
                <React.Fragment key={`${n.id}-node`}>
                    <path
                        data-id={`${n.id}`}
                        stroke="black"
                        strokeWidth="50"
                        d={`M${n.x} ${n.y} L${n.x} ${n.y}`}
                        strokeLinecap={originalData[n.id].sex === 1 ? 'square' : 'round'}
                    />
                    <path
                        stroke={originalData[n.id].affected === 1 ? 'white' : 'black'}
                        strokeWidth="45"
                        strokeLinecap={originalData[n.id].sex === 1 ? 'square' : 'round'}
                        d={`M${n.x} ${n.y} L${n.x} ${n.y}`}
                        onClick={() => click(n.id)}
                    />

                    <text
                        className="selectable"
                        data-id={`${n.id}`}
                        x={`${n.x}`}
                        y={`${n.y + 40}`}
                        stroke="white"
                        strokeWidth="2"
                        fontSize="12px"
                        textAnchor="middle"
                    >
                        {n.id}
                    </text>
                    <text
                        x={`${n.x}`}
                        y={`${n.y + 40}`}
                        fontSize="12px"
                        style={{ pointerEvents: 'none' }}
                        textAnchor="middle"
                    >
                        {n.id}
                    </text>
                </React.Fragment>
            ))}
        </svg>
    )
}
const calculateDepth = (
    person: ModifiedPedEntry,
    data: { [name: string]: ModifiedPedEntry }
): number => {
    if (!person.children.length) {
        return 1
    }

    return max(person.children.map((i) => 1 + calculateDepth(data[i], data))) ?? 1
}

// const calculateAllDescendents = (person, data) => {
//     if (!person.children.length) {
//         return person.individual_id;
//     }

//     return [
//         ...person.children,
//         ...person.children.map((i) => calculateAllDescendents(data[i], data)),
//     ].flat();
// };

/* eslint-disable no-restricted-syntax */
const findInHeirarchy = (id: string, heirarchy: Record<string, any>) => {
    for (const [index, level] of heirarchy.entries()) {
        for (const person of level) {
            if (person.id === id) {
                return index
            }
        }
    }
    return -1
}
/* eslint-enable no-restricted-syntax */

const formatData = (data: PedigreeEntry[]) => {
    if (!data.length) {
        return []
    }

    const possibleRoots = data
        .filter((item) => !item.paternal_id && !item.maternal_id)
        .map((i) => i.individual_id)
    const dataWithChildren: ModifiedPedEntry[] = data.map((item) => ({
        ...item,
        children: data
            .filter(
                (i) => i.paternal_id === item.individual_id || i.maternal_id === item.individual_id
            )
            .map((j) => j.individual_id)
            .sort(),
    }))
    const keyedData: { [name: string]: ModifiedPedEntry } = {}
    dataWithChildren.forEach((d) => {
        keyedData[d.individual_id] = d
    })

    let couples = Array.from(
        new Set(
            data
                .map((item) => `${item.paternal_id}+${item.maternal_id}`)
                .filter((item) => item !== 'null+null')
        )
    ).map((i) => i.split('+'))

    const rootLengths: [string, number][] = possibleRoots.map((item) => [
        item,
        calculateDepth(keyedData[item], keyedData),
    ])
    const maxLength = max(rootLengths.map((i) => i[1]))
    const bestRoots = rootLengths
        .filter((i) => i[1] === maxLength)
        .map((i) => i[0])
        .sort()

    const yetToSee = new Set(data.map((i) => i.individual_id))
    let queue = [[bestRoots[0]]]

    const toReturn: { id: string; parentsList?: string[] }[][] = []

    /* eslint no-loop-func: 0 */

    // create 1 lineage spine
    while (queue.flat().length) {
        const toAdd: { id: string; parentsList?: string[] }[] = []
        const toAddToQueue: string[] = []
        const nextList = queue.shift() ?? []
        nextList.forEach((next) => {
            if (!yetToSee.has(next)) {
                return
            }
            yetToSee.delete(next)
            toAdd.push({
                id: next,
                ...((keyedData[next].paternal_id || keyedData[next].maternal_id) && {
                    parentsList: [keyedData[next].paternal_id, keyedData[next].maternal_id].filter(
                        (i) => i
                    ),
                }),
            }) // add entry
            toAddToQueue.push(...keyedData[next].children.filter((i) => yetToSee.has(i)))
        })
        queue = [...queue, toAddToQueue]
        toReturn.push(toAdd)
    }

    // try add missing people
    let missingPeople = [...yetToSee]
    let updatedList = false
    while (missingPeople.length) {
        missingPeople = [...yetToSee]
        for (let i = 0; i < missingPeople.length; i += 1) {
            const next = missingPeople[i]
            // try add spouses
            const checkCouples = couples
                .filter(([dad, mum]) => dad === next || mum === next)
                .map(([dad, mum]) => (dad === next ? mum : dad))
            /* eslint-disable @typescript-eslint/no-loop-func */
            checkCouples.forEach((n) => {
                const partnerLevel = findInHeirarchy(n, toReturn)
                if (partnerLevel > -1) {
                    const partnerPosition = toReturn[partnerLevel].findIndex((j) => j.id === n)
                    // add spouse in next to partner
                    toReturn[partnerLevel].splice(partnerPosition + 1, 0, {
                        id: next,
                        ...((keyedData[next].paternal_id || keyedData[next].maternal_id) && {
                            parentsList: [
                                keyedData[next].paternal_id,
                                keyedData[next].maternal_id,
                            ].filter((k) => k),
                        }),
                    })
                    couples = couples.filter(
                        ([dad, mum]) => !([next, n].includes(dad) && [next, n].includes(mum))
                    )
                    updatedList = true
                    yetToSee.delete(next)
                }
            })
            /* eslint-enable @typescript-eslint/no-loop-func */
            if (updatedList) {
                break
            }
            // try add person above children
            const levels = keyedData[next].children
                .map((n) => findInHeirarchy(n, toReturn))
                .filter((a) => a > -1)
            if (levels.length) {
                toReturn[min(levels) - 1] = [
                    ...toReturn[min(levels) - 1],
                    {
                        id: next,
                        ...((keyedData[next].paternal_id || keyedData[next].maternal_id) && {
                            parentsList: [
                                keyedData[next].paternal_id,
                                keyedData[next].maternal_id,
                            ].filter((l) => l),
                        }),
                    },
                ]
                yetToSee.delete(next)
                updatedList = true
                break
            }
            // try add child below parents
            const parentLevel = max([
                findInHeirarchy(keyedData[next].maternal_id, toReturn),
                findInHeirarchy(keyedData[next].paternal_id, toReturn),
            ])
            if (parentLevel > -1) {
                toReturn[parentLevel + 1] = [
                    ...toReturn[parentLevel + 1],
                    {
                        id: next,
                        ...((keyedData[next].paternal_id || keyedData[next].maternal_id) && {
                            parentsList: [
                                keyedData[next].paternal_id,
                                keyedData[next].maternal_id,
                            ].filter((m) => m),
                        }),
                    },
                ]
                yetToSee.delete(next)
                updatedList = true
                break
            }
        }
        if (!updatedList) {
            break
        }
        updatedList = false
    }

    const nodeDefaults = {
        x: 0,
        y: 0,
        parents: [],
        level: -1,
        bundles: [],
        links: [],
        parentsList: [],
    }

    return toReturn.map((l) => ({
        nodes: l.map((m) => ({ ...nodeDefaults, ...m })),
        bundles: [],
    }))
}

interface RenderPedigreeProps {
    data: PedigreeEntry[]
    click?(e: string): void
}

const TangledTree: React.FunctionComponent<RenderPedigreeProps> = ({ data, click }) => {
    const [tree, setTree] = React.useState<NodeList[]>([])
    const [keyedData, setKeyedData] = React.useState<{
        [name: string]: PedigreeEntry
    }>()
    const onClick =
        click ??
        function defaultClick() {
            return undefined
        }

    React.useEffect(() => {
        setTree(formatData(data))
        setKeyedData(
            data.reduce((obj: { [key: string]: PedigreeEntry }, s: PedigreeEntry) => {
                obj[s.individual_id] = s
                return obj
            }, {})
        )
    }, [data])

    return (
        <>
            {(!data || !data.length) && (
                <MuckError message={`Ah Muck, there's no data for this pedigree!`} />
            )}
            {data && !!data.length && !tree && (
                <>
                    <LoadingDucks />
                    <div style={{ textAlign: 'center' }}>
                        <h5>Loading pedigrees</h5>
                    </div>
                </>
            )}
            {!!data.length && tree && keyedData && renderChart(tree, keyedData, onClick)}
            <br />
        </>
    )
}

export default TangledTree
