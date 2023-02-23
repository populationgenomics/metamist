/* Inspired by https://observablehq.com/@nitaku/tangled-tree-visualization-ii */

/* eslint-disable no-param-reassign */
import * as React from 'react'

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
    click?(e: string): void
}

const TangledTree: React.FunctionComponent<RenderPedigreeProps> = ({ data, click }) => (
    <>
        Temporary Pedigree Stub component
        {JSON.stringify(data)}
        {JSON.stringify(click)}
    </>
)

export default TangledTree
