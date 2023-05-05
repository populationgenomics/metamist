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
        Temporary Pedigree component
        <br />
        {`Truncated Pedigree: ' ${JSON.stringify(data.slice(0, 10))}`}
        <br />
        {JSON.stringify(click)}
    </>
)

export default TangledTree
