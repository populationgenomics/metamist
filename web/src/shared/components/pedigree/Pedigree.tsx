import * as React from 'react'

import { useQuery } from '@apollo/client'
import LoadingDucks from '../LoadingDucks/LoadingDucks'
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

interface IPedigreeProps {
  familyId: number
  onClick?: (e: string) => void
}

const Pedigree: React.FC<IPedigreeProps> = ({ familyId, onClick }) => {
  const { loading, error, data } = useQuery(GET_PEDIGREE_INFO, {
    variables: { family_id: familyId },
  })

  if (loading) return <LoadingDucks />
  if (error) return <>Error! {error.message}</>

  if (!data?.family?.project?.pedigree) return <></>

  return <TangledTree data={data.family.project.pedigree} />
}

export default Pedigree
