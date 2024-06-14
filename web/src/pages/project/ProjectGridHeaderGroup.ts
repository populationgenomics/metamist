import { MetaSearchEntityPrefix, ProjectParticipantGridFilter } from '../../sm-api'

export const headerGroupOrder = [
    MetaSearchEntityPrefix.F,
    MetaSearchEntityPrefix.P,
    MetaSearchEntityPrefix.S,
    MetaSearchEntityPrefix.Sg,
    MetaSearchEntityPrefix.A,
]

export const metaSeachEntityPrefixToFilterKey: (
    prefix: MetaSearchEntityPrefix
) => keyof ProjectParticipantGridFilter = (prefix) => {
    switch (prefix) {
        case MetaSearchEntityPrefix.F:
            return 'family'
        case MetaSearchEntityPrefix.P:
            return 'participant'
        case MetaSearchEntityPrefix.S:
            return 'sample'
        case MetaSearchEntityPrefix.Sg:
            return 'sequencing_group'
        case MetaSearchEntityPrefix.A:
            return 'assay'
    }
}

export const metaSearchEntityToTitle: (prefix: MetaSearchEntityPrefix) => string = (prefix) => {
    switch (prefix) {
        case MetaSearchEntityPrefix.F:
            return 'Family'
        case MetaSearchEntityPrefix.P:
            return 'Participant'
        case MetaSearchEntityPrefix.S:
            return 'Sample'
        case MetaSearchEntityPrefix.Sg:
            return 'Sequencing Group'
        case MetaSearchEntityPrefix.A:
            return 'Assay'
    }
}
