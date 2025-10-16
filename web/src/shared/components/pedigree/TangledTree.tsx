/* Inspired by https://observablehq.com/@nitaku/tangled-tree-visualization-ii */

/* eslint-disable no-param-reassign */
import _ from 'lodash'
import * as React from 'react'

import { extent, max, mean, min, sum } from 'd3'
import MuckError from '../MuckError'

const DECEASED_NODE_INSET_FACTOR = 2.5

export interface PedigreeEntry {
    affected: number
    family_id: string
    individual_id: string
    maternal_id?: string | null
    paternal_id?: string | null
    sex: number
    deceased?: boolean
}

interface ModifiedPedEntry extends PedigreeEntry {
    children: string[]
}

interface NodePosition {
    x: number
    y: number
}
interface Node extends NodePosition {
    id: string
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

interface NodeParentsList {
    id: string
    parentsList?: string[]
}

// layout
const defaultNodeDiameter = 40

const textColor = 'var(--color-text-primary)'
const colLines = 'var(--color-border-color)'

const colAffected = 'var(--color-pedigree-affected)'
const colPersonBorder = 'var(--color-pedigree-person-border)'
const colUnaffected = 'var(--color-pedigree-unaffected)'

interface ITangleLayoutOptions {
    nodeDiameter: number
    horizontalSpacing?: number
    verticalSpacing?: number
}

function formObjectFromIdAndParents(
    id: string,
    parents: (string | null | undefined)[]
): NodeParentsList {
    const obj: NodeParentsList = { id }

    const pl = parents.filter((i): i is string => !!i)
    if (pl.length) {
        obj.parentsList = pl
    }

    return obj
}

const constructTangleLayout = (levels: NodeList[], options: ITangleLayoutOptions) => {
    const nodes_index: Record<string, Node> = {}

    const nodeDiameter = options.nodeDiameter ?? defaultNodeDiameter
    const _horizontalSpacing = options.horizontalSpacing ?? nodeDiameter * 2.5
    const _verticalSpacing = options.verticalSpacing ?? Math.max(50, nodeDiameter * 1.7)

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
        const index: Record<string, Node> = {}
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
                    parentsList: [],
                    bundles: [],
                    links: [],
                    x: 0,
                    y: 0,
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
                    y1: d.y - _verticalSpacing / 2,
                    x2: d.x,
                    y2: d.y - _verticalSpacing / 2,
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

    let x_offset = 0
    let y_offset = 0
    levels.forEach((l) => {
        x_offset = 0
        l.nodes.forEach((n) => {
            n.x = x_offset
            n.y = y_offset

            x_offset += _horizontalSpacing
        })
        y_offset += _verticalSpacing
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
                    if (Math.abs(currentNode.x - nextNode.x) < _horizontalSpacing) {
                        moved = true
                        const oldX = nextNode.x
                        nextNode.x = currentNode.x + _horizontalSpacing
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
        l.y1 = l.source.y - _verticalSpacing / 2
        l.x2 = l.source.x
        l.y2 = l.source.y - _verticalSpacing / 2
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

    const nodeHeight = nodeDiameter + 15

    const minNodeCenterX = min(nodes, (n) => n.x) ?? 0
    const maxNodeCenterX = max(nodes, (n) => n.x) ?? 0
    const minNodeCenterY = min(nodes, (n) => n.y) ?? 0
    const maxNodeCenterY = max(nodes, (n) => n.y) ?? 0

    const layout = {
        width_dimensions: [minNodeCenterX - nodeDiameter / 2, maxNodeCenterX + nodeDiameter / 2],
        height_dimensions: [
            minNodeCenterY - nodeDiameter / 2,
            maxNodeCenterY + nodeHeight - nodeDiameter / 2,
        ],
        nodeDiameter: nodeDiameter,
        nodeHeight: nodeHeight,
        level_y_padding: _verticalSpacing,
    }

    return { levels, nodes, nodes_index, links, bundles, layout }
}

interface ITangleTreeChartProps {
    data: NodeList[]
    originalData: { [name: string]: PedigreeEntry }
    onClick?: (e: PedigreeEntry) => void
    onHighlight?: (entry?: PedigreeEntry | null) => void
    highlightedIndividual?: string | null

    nodeDiameter?: number
    nodeHorizontalSpacing?: number
    nodeVerticalSpacing?: number
    paddingX?: number
    paddingY?: number
}

const TangleTreeChart: React.FC<ITangleTreeChartProps> = (props) => {
    const {
        data,
        originalData,
        onClick,
        onHighlight,
        highlightedIndividual,
        nodeHorizontalSpacing,
        nodeVerticalSpacing,
        nodeDiameter = defaultNodeDiameter,
        paddingX = 10,
        paddingY = 10,
    } = props

    const tangleLayout = constructTangleLayout(_.cloneDeep(data), {
        nodeDiameter,
        horizontalSpacing: nodeHorizontalSpacing,
        verticalSpacing: nodeVerticalSpacing,
    })

    if ('error' in tangleLayout) {
        return <MuckError message={`Ah Muck, couldn't resolve this pedigree`} />
    }

    const minX = tangleLayout.layout.width_dimensions[0] - paddingX
    const minY = tangleLayout.layout.height_dimensions[0] - paddingY
    const width = tangleLayout.layout.width_dimensions[1] - minX + 2 * paddingX
    const height = tangleLayout.layout.height_dimensions[1] - minY + 2 * paddingY
    const viewBox = `${minX} ${minY} ${width} ${height}`

    return (
        <svg width={width} height={height} viewBox={viewBox} style={{ border: '0px solid black' }}>
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
                        {/* <path fill="none" d={`${d}`} stroke={'white'} strokeWidth="5" /> */}
                        <path d={d} fill="none" stroke={colLines} strokeWidth="2" />
                    </React.Fragment>
                )
            })}

            {tangleLayout.nodes.map((n) => (
                <PersonNode
                    key={`node-${n.id}`}
                    node={n}
                    entry={originalData[n.id]}
                    onClick={onClick}
                    onHighlight={onHighlight}
                    isHighlighted={highlightedIndividual == n.id}
                    nodeSize={nodeDiameter}
                />
            ))}
        </svg>
    )
}

interface IPersonNodeProps {
    node: NodePosition
    entry: PedigreeEntry
    isHighlighted?: boolean

    onClick?: (entry: PedigreeEntry) => void
    onHighlight?: (entry?: PedigreeEntry | null) => void

    nodeSize?: number
    showIndividualId?: boolean
}

const getSVGPathForDeceasedLine = (node: NodePosition, nodeSize: number) => {
    const insetFactor = DECEASED_NODE_INSET_FACTOR
    const mX = node.x - nodeSize / insetFactor
    const mY = node.y - nodeSize / insetFactor
    const lX = node.x + nodeSize / insetFactor
    const lY = node.y + nodeSize / insetFactor
    return `M${mX} ${mY} L${lX} ${lY}`
}

export const PersonNode: React.FC<IPersonNodeProps> = ({
    node,
    entry,
    onClick,
    onHighlight,
    isHighlighted = false,
    nodeSize = defaultNodeDiameter,
    showIndividualId = true,
}) => {
    const isDeceased = entry.deceased

    return (
        <React.Fragment>
            <g
                onMouseOver={() => {
                    onHighlight?.(entry)
                }}
                onMouseLeave={() => {
                    onHighlight?.(null)
                }}
            >
                <path
                    data-id={`${entry.individual_id}`}
                    stroke={colPersonBorder}
                    strokeWidth={isHighlighted ? nodeSize * 0.9 : nodeSize * 0.75}
                    z="-1"
                    d={`M${node.x} ${node.y} L${node.x} ${node.y}`}
                    strokeLinecap={entry.sex === 1 ? 'square' : 'round'}
                />
                <path
                    stroke={entry.affected === 2 ? colAffected : colUnaffected}
                    strokeWidth={nodeSize * 0.65}
                    strokeLinecap={entry.sex === 1 ? 'square' : 'round'}
                    d={`M${node.x} ${node.y} L${node.x} ${node.y}`}
                    onClick={() => onClick?.(entry)}
                    // on highlight
                />
                {/* if deceased, show diagonal bar through node */}
                {isDeceased && (
                    <path
                        stroke={textColor}
                        strokeWidth={nodeSize * 0.1}
                        strokeLinecap="round"
                        d={getSVGPathForDeceasedLine(node, nodeSize)}
                    />
                )}
                {/* wrap text in g to get fill to work correctly */}
                {showIndividualId && (
                    <g fill={textColor}>
                        <text
                            className="selectable"
                            data-id={`${entry.individual_id}`}
                            x={`${node.x}`}
                            y={`${node.y + nodeSize / 2 + 10}`}
                            fontSize="12px"
                            textAnchor="middle"
                            fontWeight={isHighlighted ? 'bold' : 'normal'}
                        >
                            {entry.individual_id}
                        </text>
                    </g>
                )}
            </g>
        </React.Fragment>
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
const findInHeirarchy = (id: string | null | undefined, heirarchy: NodeParentsList[][]) => {
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

    const toReturn: NodeParentsList[][] = []

    /* eslint no-loop-func: 0 */

    // create 1 lineage spine
    while (queue.flat().length) {
        const toAdd: NodeParentsList[] = []
        const toAddToQueue: string[] = []
        const nextList = queue.shift() ?? []
        nextList.forEach((next) => {
            if (!yetToSee.has(next)) {
                return
            }
            yetToSee.delete(next)
            const obj: NodeParentsList = formObjectFromIdAndParents(next, [
                keyedData[next].paternal_id,
                keyedData[next].maternal_id,
            ])
            toAdd.push(obj) // add entry
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
                    const obj: NodeParentsList = formObjectFromIdAndParents(next, [
                        keyedData[next].paternal_id,
                        keyedData[next].maternal_id,
                    ])
                    toReturn[partnerLevel].splice(partnerPosition + 1, 0, obj)
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
                const nextObj: NodeParentsList = formObjectFromIdAndParents(next, [
                    keyedData[next].paternal_id,
                    keyedData[next].maternal_id,
                ])
                toReturn[(min(levels) || 0) - 1] = [...toReturn[(min(levels) || 0) - 1], nextObj]
                yetToSee.delete(next)
                updatedList = true
                break
            }
            // try add child below parents
            const parentLevel =
                max([
                    findInHeirarchy(keyedData[next].maternal_id, toReturn),
                    findInHeirarchy(keyedData[next].paternal_id, toReturn),
                ]) || 0
            if (parentLevel > -1) {
                const nextObj: NodeParentsList = formObjectFromIdAndParents(next, [
                    keyedData[next].paternal_id,
                    keyedData[next].maternal_id,
                ])
                toReturn[parentLevel + 1] = [...toReturn[parentLevel + 1], nextObj]
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

    onClick?: (e: PedigreeEntry) => void
    onHighlight?: (e?: PedigreeEntry | null) => void

    highlightedIndividual?: string | null

    nodeDiameter?: number
    nodeHorizontalSpacing?: number
    nodeVerticalSpacing?: number
}

const TangledTree: React.FunctionComponent<RenderPedigreeProps> = ({ data, onClick, ...props }) => {
    if (!data?.length) {
        return (
            <p>
                <em>Empty pedigree</em>
            </p>
        )
    }

    const tree = formatData(data)
    const keyedData = _.keyBy(data, (s) => s.individual_id)

    return (
        <TangleTreeChart
            data={tree}
            originalData={keyedData}
            onClick={onClick}
            highlightedIndividual={props.highlightedIndividual}
            onHighlight={props.onHighlight}
            {...props}
        />
    )
}

export default TangledTree
