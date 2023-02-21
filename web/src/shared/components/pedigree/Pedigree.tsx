import * as React from 'react'

import { useQuery } from '@apollo/client'
import LoadingDucks from '../LoadingDucks'
import TangledTree from './TangledTree'

import { gql } from '../../../__generated__/gql'

const GET_PEDIGREE_INFO = gql(`
query PedigreeInfo($family_id: Int!) {
    family(familyId: $family_id) {
      project {
        pedigree(internalFamilyIds: [$family_id])
      }
    }
  }`)

const Pedigree: React.FunctionComponent<{
    familyID: number
    onClick?(e: string): void
}> = ({ familyID, onClick }) => {
    const { loading, error, data } = useQuery(GET_PEDIGREE_INFO, {
        variables: { family_id: familyID },
    })

    if (loading) return <LoadingDucks />
    if (error) return <>Error! {error.message}</>

    return data?.family.project.pedigree ? (
        <TangledTree data={data.family.project.pedigree} click={onClick} />
    ) : (
        <></>
    )
}

export default Pedigree
