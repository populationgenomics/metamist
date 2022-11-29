import * as React from "react";
import _ from "lodash";
import { min, max, descending, sum, mean, extent } from "d3";
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
    console.log(levels);
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
                    ],
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
                            ],
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
                            ],
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
                            ],
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
    // const duo = [
    //     [{ id: "Chaos" }],
    //     [{ id: "Gaea", parents: ["Chaos"] }],
    //     [
    //         { id: "Child1", parents: ["Gaea"] },
    //         { id: "Child2", parents: ["Gaea"] },
    //     ],
    // ];

    // const data3 = [
    //     [{ id: "Chaos" }],
    //     [{ id: "Gaea", parents: ["Chaos"] }, { id: "Uranus" }],
    //     [
    //         { id: "Oceanus", parents: ["Gaea", "Uranus"] },
    //         { id: "Thethys", parents: ["Gaea", "Uranus"] },
    //         { id: "Pontus" },
    //         { id: "Rhea", parents: ["Gaea", "Uranus"] },
    //         { id: "Cronus", parents: ["Gaea", "Uranus"] },
    //         { id: "Coeus", parents: ["Gaea", "Uranus"] },
    //         { id: "Phoebe", parents: ["Gaea", "Uranus"] },
    //         { id: "Crius", parents: ["Gaea", "Uranus"] },
    //         { id: "Hyperion", parents: ["Gaea", "Uranus"] },
    //         { id: "Iapetus", parents: ["Gaea", "Uranus"] },
    //         { id: "Thea", parents: ["Gaea", "Uranus"] },
    //         { id: "Themis", parents: ["Gaea", "Uranus"] },
    //         { id: "Mnemosyne", parents: ["Gaea", "Uranus"] },
    //     ],
    //     [
    //         { id: "Doris", parents: ["Oceanus", "Thethys"] },
    //         { id: "Neures", parents: ["Pontus", "Gaea"] },
    //         { id: "Dionne" },
    //         { id: "Demeter", parents: ["Rhea", "Cronus"] },
    //         { id: "Hades", parents: ["Rhea", "Cronus"] },
    //         { id: "Hera", parents: ["Rhea", "Cronus"] },
    //         { id: "Alcmene" },
    //         { id: "Zeus", parents: ["Rhea", "Cronus"] },
    //         { id: "Eris" },
    //         { id: "Leto", parents: ["Coeus", "Phoebe"] },
    //         { id: "Amphitrite" },
    //         { id: "Medusa" },
    //         { id: "Poseidon", parents: ["Rhea", "Cronus"] },
    //         { id: "Hestia", parents: ["Rhea", "Cronus"] },
    //     ],
    //     [
    //         { id: "Thetis", parents: ["Doris", "Neures"] },
    //         { id: "Peleus" },
    //         { id: "Anchises" },
    //         { id: "Adonis" },
    //         { id: "Aphrodite", parents: ["Zeus", "Dionne"] },
    //         { id: "Persephone", parents: ["Zeus", "Demeter"] },
    //         { id: "Ares", parents: ["Zeus", "Hera"] },
    //         { id: "Hephaestus", parents: ["Zeus", "Hera"] },
    //         { id: "Hebe", parents: ["Zeus", "Hera"] },
    //         { id: "Hercules", parents: ["Zeus", "Alcmene"] },
    //         { id: "Megara" },
    //         { id: "Deianira" },
    //         { id: "Eileithya", parents: ["Zeus", "Hera"] },
    //         { id: "Ate", parents: ["Zeus", "Eris"] },
    //         { id: "Leda" },
    //         { id: "Athena", parents: ["Zeus"] },
    //         { id: "Apollo", parents: ["Zeus", "Leto"] },
    //         { id: "Artemis", parents: ["Zeus", "Leto"] },
    //         { id: "Triton", parents: ["Poseidon", "Amphitrite"] },
    //         { id: "Pegasus", parents: ["Poseidon", "Medusa"] },
    //         { id: "Orion", parents: ["Poseidon"] },
    //         { id: "Polyphemus", parents: ["Poseidon"] },
    //     ],
    //     [
    //         { id: "Deidamia" },
    //         { id: "Achilles", parents: ["Peleus", "Thetis"] },
    //         { id: "Creusa" },
    //         { id: "Aeneas", parents: ["Anchises", "Aphrodite"] },
    //         { id: "Lavinia" },
    //         { id: "Eros", parents: ["Hephaestus", "Aphrodite"] },
    //         { id: "Helen", parents: ["Leda", "Zeus"] },
    //         { id: "Menelaus" },
    //         { id: "Polydueces", parents: ["Leda", "Zeus"] },
    //     ],
    //     [
    //         { id: "Andromache" },
    //         { id: "Neoptolemus", parents: ["Deidamia", "Achilles"] },
    //         { id: "Aeneas(2)", parents: ["Creusa", "Aeneas"] },
    //         { id: "Pompilius", parents: ["Creusa", "Aeneas"] },
    //         { id: "Iulus", parents: ["Lavinia", "Aeneas"] },
    //         { id: "Hermione", parents: ["Helen", "Menelaus"] },
    //     ],
    // ];

    // const data1 = [
    //     {
    //         family_id: "8600",
    //         individual_id: "D21-0075",
    //         paternal_id: null,
    //         maternal_id: null,
    //         sex: 1,
    //         affected: 1,
    //     },
    //     {
    //         family_id: "8600",
    //         individual_id: "D21-0074",
    //         paternal_id: "8600.1",
    //         maternal_id: "8600.2",
    //         sex: 2,
    //         affected: 1,
    //     },
    //     {
    //         family_id: "8600",
    //         individual_id: "D21-0076",
    //         paternal_id: "D21-0075",
    //         maternal_id: "D21-0074",
    //         sex: 2,
    //         affected: 2,
    //     },
    //     {
    //         family_id: "8600",
    //         individual_id: "D13-708",
    //         paternal_id: "8600.1",
    //         maternal_id: "8600.2",
    //         sex: 2,
    //         affected: 1,
    //     },
    //     {
    //         family_id: "8600",
    //         individual_id: "8600.1",
    //         paternal_id: null,
    //         maternal_id: null,
    //         sex: 1,
    //         affected: 1,
    //     },
    //     {
    //         family_id: "8600",
    //         individual_id: "8600.2",
    //         paternal_id: null,
    //         maternal_id: null,
    //         sex: 2,
    //         affected: 1,
    //     },
    // ];

    // const data2 = [
    //     {
    //         family_id: "CMT27",
    //         individual_id: "CMT27_050129",
    //         paternal_id: "CMT27_1",
    //         maternal_id: "CMT27_2",
    //         sex: 1,
    //         affected: 2,
    //     },
    //     {
    //         family_id: "CMT27",
    //         individual_id: "CMT27_170151",
    //         paternal_id: null,
    //         maternal_id: null,
    //         sex: 1,
    //         affected: 1,
    //     },
    //     {
    //         family_id: "CMT27",
    //         individual_id: "CMT27_140560",
    //         paternal_id: "CMT27_1",
    //         maternal_id: "CMT27_2",
    //         sex: 1,
    //         affected: 1,
    //     },
    //     {
    //         family_id: "CMT27",
    //         individual_id: "CMT27_090544",
    //         paternal_id: "CMT27_14",
    //         maternal_id: "CMT27_15",
    //         sex: 1,
    //         affected: 2,
    //     },
    //     {
    //         family_id: "CMT27",
    //         individual_id: "CMT27_050133",
    //         paternal_id: "CMT27_050129",
    //         maternal_id: "CMT27_8",
    //         sex: 2,
    //         affected: 2,
    //     },
    //     {
    //         family_id: "CMT27",
    //         individual_id: "CMT27_150022",
    //         paternal_id: "CMT27_170151",
    //         maternal_id: "CMT27_050133",
    //         sex: 2,
    //         affected: 2,
    //     },
    //     {
    //         family_id: "CMT27",
    //         individual_id: "CMT27_1",
    //         paternal_id: null,
    //         maternal_id: null,
    //         sex: 1,
    //         affected: 0,
    //     },
    //     {
    //         family_id: "CMT27",
    //         individual_id: "CMT27_2",
    //         paternal_id: null,
    //         maternal_id: null,
    //         sex: 2,
    //         affected: 0,
    //     },
    //     {
    //         family_id: "CMT27",
    //         individual_id: "CMT27_3",
    //         paternal_id: "CMT27_1",
    //         maternal_id: "CMT27_2",
    //         sex: 1,
    //         affected: 1,
    //     },
    //     {
    //         family_id: "CMT27",
    //         individual_id: "CMT27_4",
    //         paternal_id: null,
    //         maternal_id: null,
    //         sex: 2,
    //         affected: 1,
    //     },
    //     {
    //         family_id: "CMT27",
    //         individual_id: "CMT27_6",
    //         paternal_id: null,
    //         maternal_id: null,
    //         sex: 2,
    //         affected: 1,
    //     },
    //     {
    //         family_id: "CMT27",
    //         individual_id: "CMT27_8",
    //         paternal_id: null,
    //         maternal_id: null,
    //         sex: 2,
    //         affected: 1,
    //     },
    //     {
    //         family_id: "CMT27",
    //         individual_id: "CMT27_9",
    //         paternal_id: "CMT27_3",
    //         maternal_id: "CMT27_4",
    //         sex: 2,
    //         affected: 1,
    //     },
    //     {
    //         family_id: "CMT27",
    //         individual_id: "CMT27_10",
    //         paternal_id: "CMT27_140560",
    //         maternal_id: "CMT27_6",
    //         sex: 1,
    //         affected: 1,
    //     },
    //     {
    //         family_id: "CMT27",
    //         individual_id: "CMT27_11",
    //         paternal_id: "CMT27_140560",
    //         maternal_id: "CMT27_6",
    //         sex: 2,
    //         affected: 1,
    //     },
    //     {
    //         family_id: "CMT27",
    //         individual_id: "CMT27_12",
    //         paternal_id: "CMT27_140560",
    //         maternal_id: "CMT27_6",
    //         sex: 2,
    //         affected: 1,
    //     },
    //     {
    //         family_id: "CMT27",
    //         individual_id: "CMT27_13",
    //         paternal_id: "CMT27_140560",
    //         maternal_id: "CMT27_6",
    //         sex: 1,
    //         affected: 1,
    //     },
    //     {
    //         family_id: "CMT27",
    //         individual_id: "CMT27_14",
    //         paternal_id: null,
    //         maternal_id: null,
    //         sex: 1,
    //         affected: 1,
    //     },
    //     {
    //         family_id: "CMT27",
    //         individual_id: "CMT27_15",
    //         paternal_id: "CMT27_050129",
    //         maternal_id: "CMT27_8",
    //         sex: 2,
    //         affected: 2,
    //     },
    //     {
    //         family_id: "CMT27",
    //         individual_id: "CMT27_16",
    //         paternal_id: "CMT27_050129",
    //         maternal_id: "CMT27_8",
    //         sex: 2,
    //         affected: 1,
    //     },
    //     {
    //         family_id: "CMT27",
    //         individual_id: "CMT27_17",
    //         paternal_id: "CMT27_050129",
    //         maternal_id: "CMT27_8",
    //         sex: 1,
    //         affected: 2,
    //     },
    //     {
    //         family_id: "CMT27",
    //         individual_id: "CMT27_18",
    //         paternal_id: null,
    //         maternal_id: null,
    //         sex: 2,
    //         affected: 1,
    //     },
    //     {
    //         family_id: "CMT27",
    //         individual_id: "CMT27_22",
    //         paternal_id: "CMT27_14",
    //         maternal_id: "CMT27_15",
    //         sex: 2,
    //         affected: 1,
    //     },
    //     {
    //         family_id: "CMT27",
    //         individual_id: "CMT27_23",
    //         paternal_id: "CMT27_14",
    //         maternal_id: "CMT27_15",
    //         sex: 2,
    //         affected: 2,
    //     },
    //     {
    //         family_id: "CMT27",
    //         individual_id: "CMT27_24",
    //         paternal_id: "CMT27_17",
    //         maternal_id: "CMT27_18",
    //         sex: 1,
    //         affected: 0,
    //     },
    //     {
    //         family_id: "CMT27",
    //         individual_id: "CMT27_26",
    //         paternal_id: "CMT27_170151",
    //         maternal_id: "CMT27_050133",
    //         sex: 2,
    //         affected: 2,
    //     },
    //     {
    //         family_id: "CMT274",
    //         individual_id: "CMT274_120221",
    //         paternal_id: "CMT274_1",
    //         maternal_id: "CMT274_2",
    //         sex: 2,
    //         affected: 2,
    //     },
    //     {
    //         family_id: "CMT274",
    //         individual_id: "CMT274_1",
    //         paternal_id: null,
    //         maternal_id: null,
    //         sex: 1,
    //         affected: 1,
    //     },
    //     {
    //         family_id: "CMT274",
    //         individual_id: "CMT274_2",
    //         paternal_id: null,
    //         maternal_id: null,
    //         sex: 2,
    //         affected: 1,
    //     },
    // ];
    // const shuffled = duo
    //     .map((value) => ({ value, sort: Math.random() }))
    //     .sort((a, b) => a.sort - b.sort)
    //     .map(({ value }) => value);
    console.log("Here");
    const trees = formatData(data);
    console.log("There");
    const keyedData: { [name: string]: PedigreeEntry } = data.reduce(
        (obj: { [key: string]: PedigreeEntry }, s: PedigreeEntry) => {
            obj[s.individual_id] = s;
            return obj;
        },
        {}
    );
    console.log(trees[0]);

    const r = renderChart(trees[0], keyedData, click);
    console.log("Everywhere");

    return r;

    // return (
    //     <>
    //         {trees.map((tree, i) => (
    //             <React.Fragment key={`Tree-${i}`}>
    //                 {renderChart(tree, keyedData, click)}
    //             </React.Fragment>
    //         ))}
    //         <br />
    //     </>
    // );
};
