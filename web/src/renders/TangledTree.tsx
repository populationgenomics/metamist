import * as React from "react";
import _ from "lodash";
import { min, max, descending, sum, mean, extent } from "d3";
import LoadingDucks from "./LoadingDucks";
import MuckTheDuck from "./MuckTheDuck";

interface PedigreeEntry {
    affected: number;
    family_id: string;
    individual_id: string;
    maternal_id: string;
    paternal_id: string;
    sex: number;
}

interface ModifiedPedEntry {
    affected: number;
    family_id: string;
    individual_id: string;
    maternal_id: string;
    paternal_id: string;
    sex: number;
    children: string[];
}

const constructTangleLayout = (
    levels: { id: string; parents?: string[]; level?: number }[][]
) => {
    // precompute level depth
    levels.forEach((l, i) => l.forEach((n) => (n.level = i)));
    var nodes = levels.reduce((a, x) => a.concat(x), []);
    var nodes_index = {};
    nodes.forEach((d) => (nodes_index[d.id] = d));

    // objectification
    nodes.forEach((d) => {
        d.parents = (d.parents === undefined ? [] : d.parents).map(
            (p) => nodes_index[p]
        );
    });

    // precompute bundles
    levels.forEach((l, i) => {
        var index = {};
        l.forEach((n) => {
            if (n.parents.length === 0) {
                return;
            }
            var id = n.parents
                .map((d) => d.id)
                .sort()
                .join("-X-");
            if (id in index) {
                index[id].parents = index[id].parents.concat(n.parents);
            } else {
                index[id] = {
                    id: id,
                    parents: n.parents.slice(),
                    level: i,
                    span: i - min(n.parents, (p) => p.level),
                };
            }
            n.bundle = index[id];
        });
        l.bundles = Object.keys(index).map((k) => index[k]);
        l.bundles.forEach((b, i) => (b.i = i));
    });

    var links = [];
    nodes.forEach((d) => {
        d.parents.forEach((p) =>
            links.push({ source: d, bundle: d.bundle, target: p })
        );
    });

    var bundles = levels.reduce((a, x) => a.concat(x.bundles), []);

    // reverse pointer from parent to bundles
    bundles.forEach((b) =>
        b.parents.forEach((p) => {
            if (p.bundles_index === undefined) {
                p.bundles_index = {};
            }
            if (!(b.id in p.bundles_index)) {
                p.bundles_index[b.id] = [];
            }
            p.bundles_index[b.id].push(b);
        })
    );

    nodes.forEach((n) => {
        if (n.bundles_index !== undefined) {
            n.bundles = Object.keys(n.bundles_index).map(
                (k) => n.bundles_index[k]
            );
        } else {
            n.bundles_index = {};
            n.bundles = [];
        }
        n.bundles.sort((a, b) =>
            descending(
                max(a, (d) => d.span),
                max(b, (d) => d.span)
            )
        );
        n.bundles.forEach((b, i) => (b.i = i));
    });

    links.forEach((l) => {
        if (l.bundle.links === undefined) {
            l.bundle.links = [];
        }
        l.bundle.links.push(l);
    });

    // layout
    const yPadding = 50;
    const xPadding = 50;
    const node_height = 22;
    const node_width = 70;
    const bundle_width = 14;
    const level_y_padding = 100;
    const horizontal_spacing = 100;

    nodes.forEach((n) => (n.height = Math.max(1, n.bundles.length) - 1));

    var x_offset = xPadding;
    var y_offset = yPadding;
    levels.forEach((l) => {
        x_offset = xPadding;
        l.forEach((n, i) => {
            n.x = x_offset;
            n.y = y_offset;

            x_offset += horizontal_spacing;
        });
        y_offset += level_y_padding;
    });

    const rebalanceNodes = () => {
        let moved = false;
        let movedBundles: string[] = [];
        let movedChildren: string[] = [];
        levels.forEach((level) => {
            level.forEach((currentNode, movedIndex) => {
                const oldLevel = [...level];
                level.sort(
                    (a, b) =>
                        a.x - b.x ||
                        oldLevel.findIndex((c) => a.id === c.id) -
                            oldLevel.findIndex((c) => b.id === c.id)
                );
                if (movedIndex < level.length - 1) {
                    //not the last node
                    const nextNode = level[movedIndex + 1];
                    if (
                        Math.abs(currentNode.x - nextNode.x) <
                        horizontal_spacing
                    ) {
                        moved = true;
                        const oldX = nextNode.x;
                        nextNode.x = currentNode.x + horizontal_spacing;
                        const movedX = nextNode.x - oldX;
                        nextNode.bundles.forEach((bundle) => {
                            bundle
                                .filter((b) => !movedBundles.includes(b.id))
                                .forEach((b) => {
                                    movedBundles.push(b.id);
                                    b.links.forEach((l) => {
                                        l.xs += movedX;
                                        l.xb += movedX;
                                        if (
                                            movedChildren.includes(l.source.id)
                                        ) {
                                            return;
                                        }
                                        nodes_index[l.source.id].x += movedX;
                                        movedChildren.push(l.source.id);
                                        l.x1 += movedX;
                                        l.x2 += movedX;
                                        // l.xt += movedX;
                                    });
                                });
                        });
                    }
                }
            });
        });
        return moved;
    };

    levels.reverse().forEach((l) => {
        let seenBundles = [];
        l.forEach((node) => {
            if (!("bundle" in node) || seenBundles.includes(node.bundle.id)) {
                return;
            }
            const minXParent = min(node.parents.map((p) => p.x));
            if (minXParent < node.x) {
                const amountToMove = node.x - minXParent;
                node.parents.forEach((p) => (p.x += amountToMove));
                const maxParent = node.parents.reduce((prev, current) => {
                    return prev.x > current.x ? prev : current;
                });
                //move rest of level accordingly
                const currentLevel = maxParent.level;
                const movedIndex = nodes.findIndex(
                    (b) => b.id === maxParent.id
                );
                nodes.forEach((n, i) => {
                    if (n.level === currentLevel && i > movedIndex) {
                        n.x += amountToMove;
                    }
                });
            }
            seenBundles.push(node.bundle.id);
        });
    });

    levels.forEach((l) => {
        l.bundles.forEach((b) => {
            b.x = sum(b.parents, (d) => d.x) / b.parents.length;
            b.y = sum(b.parents, (d) => d.y) / b.parents.length;
        });
    });

    links.forEach((l) => {
        l.xt = l.target.x;
        l.yt = l.target.y;
        l.xb = l.bundle.x;
        l.yb = l.bundle.y;
        l.x1 = l.bundle.x;
        l.y1 = l.source.y - level_y_padding / 2;
        l.x2 = l.source.x;
        l.y2 = l.source.y - level_y_padding / 2;
        l.xs = l.source.x;
        l.ys = l.source.y;
    });

    //try centre parents
    let moved = true;
    let numLoops = 0;
    while (moved && numLoops < 1000) {
        moved = false;
        levels.reverse().forEach((l) => {
            l.bundles.forEach((b) => {
                const avgX = mean(extent(b.links.map((l) => l.source.x)));
                b.links.forEach((p) => {
                    if (p.x1 !== avgX) {
                        const oldxb = p.xb;
                        p.xb = avgX;
                        p.xt += avgX - oldxb;
                        nodes_index[p.target.id].x = p.xt;
                        p.x1 = avgX;
                        moved = true;
                    }
                });
            });
        });
        moved = moved || rebalanceNodes();
        links.forEach((l) => {
            l.xt = l.target.x;
            l.yt = l.target.y;
            l.x2 = l.source.x;
            l.xs = l.source.x;
            l.ys = l.source.y;
        });
        if (!moved) {
            break;
        }
        numLoops++;
    }

    if (numLoops === 1000) {
        return { error: "Infinite Loop" };
    }

    const width_dim = extent(nodes, (n) => n.x);
    const height_dim = extent(nodes, (n) => n.y);
    var layout = {
        width_dimensions: [
            width_dim[0] - node_width,
            width_dim[1] + node_width + 2 * xPadding,
        ],
        height_dimensions: [
            height_dim[0] - node_height / 2 - yPadding,
            height_dim[1] + node_height / 2 + 2 * yPadding,
        ],
        node_height,
        node_width,
        bundle_width,
        level_y_padding,
    };

    return { levels, nodes, nodes_index, links, bundles, layout };
};

const renderChart = (
    data: { id: string; parents?: string[] }[][],
    originalData: { [name: string]: PedigreeEntry },
    click: (e: string) => void
) => {
    const tangleLayout = constructTangleLayout(_.cloneDeep(data));

    if ("error" in tangleLayout) {
        return (
            <p>
                <em>Ah Muck, couldn't resolve this pedigree</em>
                <MuckTheDuck height={28} style={{ transform: "scaleY(-1)" }} />
            </p>
        );
    }

    return (
        <svg
            width={`${
                tangleLayout.layout.width_dimensions[1] -
                tangleLayout.layout.width_dimensions[0]
            }`}
            height={`${
                tangleLayout.layout.height_dimensions[1] -
                tangleLayout.layout.height_dimensions[0]
            }`}
            viewBox={`${tangleLayout.layout.width_dimensions[0]} ${tangleLayout.layout.height_dimensions[0]} ${tangleLayout.layout.width_dimensions[1]} ${tangleLayout.layout.height_dimensions[1]}`}
        >
            {tangleLayout.bundles.map((b, i) => {
                let d = b.links
                    .map(
                        (l) => `
                        M ${l.xt} ${l.yt}
                        L ${l.xb} ${l.yb}
                        L ${l.x1} ${l.y1}
                        L ${l.x2} ${l.y2}
                        L ${l.xs} ${l.ys}`
                    )
                    .join("");
                return (
                    <React.Fragment key={`${b.id}-bundle`}>
                        <path
                            fill="none"
                            d={`${d}`}
                            stroke={"white"}
                            strokeWidth="5"
                        />
                        <path
                            d={`${d}`}
                            fill="none"
                            stroke={"black"}
                            strokeWidth="2"
                        />
                    </React.Fragment>
                );
            })}

            {tangleLayout.nodes.map((n) => (
                <React.Fragment key={`${n.id}-node`}>
                    <path
                        data-id={`${n.id}`}
                        stroke="black"
                        strokeWidth="50"
                        d={`M${n.x} ${n.y} L${n.x} ${n.y}`}
                        strokeLinecap={
                            originalData[n.id].sex === 1 ? "square" : "round"
                        }
                    />
                    <path
                        stroke={
                            originalData[n.id].affected === 1
                                ? "white"
                                : "black"
                        }
                        strokeWidth="45"
                        strokeLinecap={
                            originalData[n.id].sex === 1 ? "square" : "round"
                        }
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
                        style={{ pointerEvents: "none" }}
                        textAnchor="middle"
                    >
                        {n.id}
                    </text>
                </React.Fragment>
            ))}
        </svg>
    );
};
const calculateDepth = (
    person: ModifiedPedEntry,
    data: { [name: string]: ModifiedPedEntry }
): number => {
    if (!person.children.length) {
        return 1;
    }

    return (
        max(person.children.map((i) => 1 + calculateDepth(data[i], data))) ?? 1
    );
};

// const calculateAllDescendents = (person, data) => {
//     if (!person.children.length) {
//         return person.individual_id;
//     }

//     return [
//         ...person.children,
//         ...person.children.map((i) => calculateAllDescendents(data[i], data)),
//     ].flat();
// };

const findInHeirarchy = (id, heirarchy) => {
    for (const [index, level] of heirarchy.entries()) {
        for (const person of level) {
            // console.log(person);
            if (person.id === id) {
                return index;
            }
        }
    }
    return -1;
};

const formatData = (data: PedigreeEntry[]) => {
    if (!data.length) {
        return [];
    }

    const possibleRoots = data
        .filter((item) => !item.paternal_id && !item.maternal_id)
        .map((i) => i.individual_id);
    const dataWithChildren: ModifiedPedEntry[] = data.map((item) => ({
        ...item,
        children: data
            .filter(
                (i) =>
                    i.paternal_id === item.individual_id ||
                    i.maternal_id === item.individual_id
            )
            .map((item) => item.individual_id)
            .sort(),
    }));
    var keyedData: { [name: string]: ModifiedPedEntry } = {};
    dataWithChildren.forEach((d) => (keyedData[d.individual_id] = d));

    let couples = Array.from(
        new Set(
            data
                .map((item) => `${item.paternal_id}+${item.maternal_id}`)
                .filter((item) => item !== "null+null")
        )
    ).map((i) => i.split("+"));

    const rootLengths: [string, number][] = possibleRoots.map((item) => {
        return [item, calculateDepth(keyedData[item], keyedData)];
    });
    const maxLength = max(rootLengths.map((i) => i[1]));
    const bestRoots = rootLengths
        .filter((i) => i[1] === maxLength)
        .map((i) => i[0])
        .sort();

    const yetToSee = new Set(data.map((i) => i.individual_id));
    let queue = [[bestRoots[0]]];

    let toReturn = [];

    /*eslint no-loop-func: 0*/

    //create 1 lineage spine
    while (queue.flat().length) {
        let toAdd: { id: string; parents?: string[] }[] = [];
        let toAddToQueue: string[] = [];
        const nextList = queue.shift()!;
        nextList.forEach((next) => {
            if (!yetToSee.has(next)) {
                return;
            }
            yetToSee.delete(next);
            toAdd.push({
                id: next,
                ...((keyedData[next].paternal_id ||
                    keyedData[next].maternal_id) && {
                    parents: [
                        keyedData[next].paternal_id,
                        keyedData[next].maternal_id,
                    ].filter((i) => i),
                }),
            }); //add entry
            toAddToQueue.push(
                ...keyedData[next].children.filter((i) => yetToSee.has(i))
            );
        });
        queue = [...queue, toAddToQueue];
        toReturn.push(toAdd);
    }

    //try add missing people
    let missingPeople = [...yetToSee];
    let updatedList = false;
    while (missingPeople.length) {
        missingPeople = [...yetToSee];
        for (let i = 0; i < missingPeople.length; i++) {
            const next = missingPeople[i];
            //try add spouses
            const checkCouples = couples
                .filter(([dad, mum]) => dad === next || mum === next)
                .map(([dad, mum]) => (dad === next ? mum : dad));
            checkCouples.forEach((n) => {
                const partnerLevel = findInHeirarchy(n, toReturn);
                if (partnerLevel > -1) {
                    const partnerPosition = toReturn[partnerLevel].findIndex(
                        (i) => i.id === n
                    );
                    //add spouse in next to partner
                    toReturn[partnerLevel].splice(partnerPosition + 1, 0, {
                        id: next,
                        ...((keyedData[next].paternal_id ||
                            keyedData[next].maternal_id) && {
                            parents: [
                                keyedData[next].paternal_id,
                                keyedData[next].maternal_id,
                            ].filter((i) => i),
                        }),
                    });
                    couples = couples.filter(
                        ([dad, mum]) =>
                            !(
                                [next, n].includes(dad) &&
                                [next, n].includes(mum)
                            )
                    );
                    updatedList = true;
                    yetToSee.delete(next);
                }
            });
            if (updatedList) {
                break;
            }
            //try add person above children
            const levels = keyedData[next].children
                .map((n) => findInHeirarchy(n, toReturn))
                .filter((a) => a > -1);
            if (levels.length) {
                toReturn[min(levels) - 1] = [
                    ...toReturn[min(levels) - 1],
                    {
                        id: next,
                        ...((keyedData[next].paternal_id ||
                            keyedData[next].maternal_id) && {
                            parents: [
                                keyedData[next].paternal_id,
                                keyedData[next].maternal_id,
                            ].filter((i) => i),
                        }),
                    },
                ];
                yetToSee.delete(next);
                updatedList = true;
                break;
            }
            //try add child below parents
            const parentLevel = max([
                findInHeirarchy(keyedData[next].maternal_id, toReturn),
                findInHeirarchy(keyedData[next].paternal_id, toReturn),
            ]);
            if (parentLevel > -1) {
                toReturn[parentLevel + 1] = [
                    ...toReturn[parentLevel + 1],
                    {
                        id: next,
                        ...((keyedData[next].paternal_id ||
                            keyedData[next].maternal_id) && {
                            parents: [
                                keyedData[next].paternal_id,
                                keyedData[next].maternal_id,
                            ].filter((i) => i),
                        }),
                    },
                ];
                yetToSee.delete(next);
                updatedList = true;
                break;
            }
        }
        if (!updatedList) {
            break;
        }
        updatedList = false;
    }

    return yetToSee.size
        ? [
              toReturn,
              ...formatData(data.filter((i) => yetToSee.has(i.individual_id))),
          ]
        : [toReturn];
};

interface RenderPedigreeProps {
    data: PedigreeEntry[];
    click(e: string): void;
}

export const TangledTree: React.FunctionComponent<RenderPedigreeProps> = ({
    data,
    click,
}) => {
    const [trees, setTrees] = React.useState();
    const [keyedData, setKeyedData] = React.useState<{
        [name: string]: PedigreeEntry;
    }>();

    React.useEffect(() => {
        setTrees(formatData(data));
        setKeyedData(
            data.reduce(
                (obj: { [key: string]: PedigreeEntry }, s: PedigreeEntry) => {
                    obj[s.individual_id] = s;
                    return obj;
                },
                {}
            )
        );
    }, [data]);

    return (
        <>
            {(!data || !data.length) && (
                <p>
                    <em>Ah Muck, there's no data for this pedigree!</em>
                    <MuckTheDuck
                        height={28}
                        style={{ transform: "scaleY(-1)" }}
                    />
                </p>
            )}
            {data && !!data.length && !trees && (
                <>
                    <LoadingDucks />
                    <div style={{ textAlign: "center" }}>
                        <h5>Loading pedigrees</h5>
                    </div>
                </>
            )}
            {!!data.length &&
                trees &&
                keyedData &&
                trees.map((tree, i) => (
                    <React.Fragment key={`Tree-${i}`}>
                        {trees.length > 1 && <h2>{`Pedigree #${i + 1}:`}</h2>}
                        {renderChart(tree, keyedData, click)}
                        <br />
                    </React.Fragment>
                ))}
            <br />
        </>
    );
};
