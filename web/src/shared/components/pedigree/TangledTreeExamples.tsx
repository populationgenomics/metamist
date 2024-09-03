import * as React from 'react'

import TangledTree, { PedigreeEntry } from './TangledTree'

const basicTrio: PedigreeEntry[] = [
    {
        family_id: 'TRIO',
        individual_id: 'Parent_1',
        paternal_id: null,
        maternal_id: null,
        sex: 1,
        affected: 2,
    },
    {
        family_id: 'TRIO',
        individual_id: 'Parent_2',
        paternal_id: null,
        maternal_id: null,
        sex: 2,
        affected: 2,
    },
    {
        family_id: 'TRIO',
        individual_id: 'Trio_Child',
        paternal_id: 'Parent_1',
        maternal_id: 'Parent_2',
        sex: 1,
        affected: 1,
    },
]

const fourGenTree: PedigreeEntry[] = [
    {
        family_id: 'Gen1',
        individual_id: 'Grandparent_1',
        paternal_id: null,
        maternal_id: null,
        sex: 1,
        affected: 2,
    },
    {
        family_id: 'Gen1',
        individual_id: 'Grandparent_2',
        paternal_id: null,
        maternal_id: null,
        sex: 2,
        affected: 2,
    },
    {
        family_id: 'Gen1',
        individual_id: 'Grandparent_3',
        paternal_id: null,
        maternal_id: null,
        sex: 1,
        affected: 2,
    },
    {
        family_id: 'Gen2',
        individual_id: 'Parent_1',
        paternal_id: 'Grandparent_1',
        maternal_id: 'Grandparent_2',
        sex: 1,
        affected: 1,
    },
    {
        family_id: 'Gen2',
        individual_id: 'Parent_2',
        paternal_id: 'Grandparent_3',
        sex: 2,
        affected: 2,
    },
    {
        family_id: 'Gen3',
        individual_id: 'Child_1',
        paternal_id: 'Parent_1',
        maternal_id: 'Parent_2',
        sex: 1,
        affected: 2,
    },
    {
        family_id: 'Gen3',
        individual_id: 'Child_2',
        paternal_id: 'Parent_1',
        maternal_id: 'Parent_2',
        sex: 2,
        affected: 1,
    },
    {
        family_id: 'Gen3',
        individual_id: 'Child_3',
        paternal_id: 'Parent_1',
        maternal_id: 'Parent_2',
        sex: 1,
        affected: 2,
    },
    {
        family_id: 'Gen3',
        individual_id: 'Unrelated_Child1',
        sex: 2,
        affected: 2,
    },
    {
        family_id: 'Gen3',
        individual_id: 'Unrelated_Child2',
        sex: 1,
        affected: 1,
    },
    {
        family_id: 'Gen4',
        individual_id: 'Grandchild_1',
        paternal_id: 'Child_1',
        maternal_id: 'Unrelated_Child1',
        sex: 1,
        affected: 2,
    },
    {
        family_id: 'Gen4',
        individual_id: 'Grandchild_2',
        paternal_id: 'Child_1',
        maternal_id: 'Unrelated_Child1',
        sex: 2,
        affected: 1,
    },
    {
        family_id: 'Gen4',
        individual_id: 'Grandchild_3',
        paternal_id: 'Child_1',
        maternal_id: 'Unrelated_Child1',
        sex: 1,
        affected: 1,
    },
    {
        family_id: 'Gen4',
        individual_id: 'Grandchild_4',
        paternal_id: 'Unrelated_Child2',
        maternal_id: 'Child_2',
        sex: 2,
        affected: 2,
    },
]

export const TangledTreeExamples: React.FC = () => {
    const [nodeDiameter, setNodeDiameter] = React.useState<number>(40)

    return (
        <>
            <p>Node diameter {nodeDiameter}</p>
            <input
                type="range"
                min="10"
                max="100"
                value={nodeDiameter}
                onChange={(e) => setNodeDiameter(parseInt(e.target.value))}
            />
            <div style={{ border: '1px' }}>
                <TangledTree data={basicTrio} nodeDiameter={nodeDiameter} />
            </div>
            <br />
            <div style={{ border: '1px' }}>
                <TangledTree data={fourGenTree} nodeDiameter={nodeDiameter} />
            </div>
        </>
    )
}
