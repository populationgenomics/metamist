import * as yaml from 'js-yaml'
import {
    MetaSearchEntityPrefix,
    ProjectParticipantGridField,
    ProjectParticipantGridFilter,
} from '../../sm-api'
import { DictEditor, DictEditorInput } from './DictEditor'
import { metaSeachEntityPrefixToFilterKey } from './ProjectGridHeaderGroup'

interface IProjectGridFilterGuideProps {
    headerGroups: Record<MetaSearchEntityPrefix, ProjectParticipantGridField[]>
    filterValues?: ProjectParticipantGridFilter
    updateFilters: (filters: Partial<ProjectParticipantGridFilter>) => void
}

const potentialFields = {
    eq: '<value>',
    in_: ['<value1>', '<value2>'],
    nin: ['value1', 'value2'],
    gt: '<value>',
    gte: '<value>',
    lt: '<value>',
    lte: '<value>',
    contains: '<value>',
    icontains: '<vALuE>',
    startswith: '<value>',
}

const exampleDictionary = {
    participant: {
        externalId: {
            eq: 'participant_id',
            in_: ['participant_id'],
            nin: ['participant_id2'],
            contains: 'ticipant_',
            icontains: 'pARticIPaNt_iD',
            startswith: 'participant_i',
        },
    },
    sample: {
        meta: {
            numericalField: { gt: 0, gte: 1, lt: 2, lte: 1 },
        },
    },
}

export const ProjectGridFilterGuide: React.FC<IProjectGridFilterGuideProps> = ({
    headerGroups,
    filterValues,
    updateFilters,
}) => {
    const fullDictionary: { [key in keyof ProjectParticipantGridFilter]: unknown } = {}

    const keys: MetaSearchEntityPrefix[] = Object.keys(headerGroups) as MetaSearchEntityPrefix[]
    for (const key of keys) {
        const fields: ProjectParticipantGridField[] = headerGroups[key]
        const metaFields = fields.filter((f) => f.key.startsWith('meta.'))
        const nonMetaFields = fields.filter((f) => !f.key.startsWith('meta.'))
        const fieldMap: { [k: string]: string | { [k: string]: string } } = {}
        for (const field of nonMetaFields) {
            const filterTypes = field.filter_types?.length
                ? field.filter_types
                : Object.keys(potentialFields)
            fieldMap[field.filter_key ?? field.key] = filterTypes.join(', ')
        }

        const meta = metaFields.reduce(
            (metaAcc, metaField) => {
                const filterTypes = metaField.filter_types?.length
                    ? metaField.filter_types
                    : Object.keys(potentialFields)

                metaAcc[(metaField.filter_key ?? metaField.key).substring(5)] =
                    filterTypes.join(', ')
                return metaAcc
            },
            {} as { [k: string]: string }
        )

        if (Object.keys(meta).length > 0) {
            fieldMap.meta = meta
        }

        const f = metaSeachEntityPrefixToFilterKey(key)

        fullDictionary[f] = fieldMap
    }

    const yamlString = yaml.dump(fullDictionary)

    return (
        <>
            {filterValues && (
                <>
                    <h2>Project grid filter</h2>
                    <p>
                        <em>
                            This box below is the filter for the project grid, making changes here
                            and clicking apply will cause the filter to reload. Make sure to hit
                            apply BEFORE clicking away from this modal.
                        </em>
                    </p>
                    <DictEditor input={filterValues as DictEditorInput} onChange={updateFilters} />
                    <br />
                    <hr />
                </>
            )}
            <h2>How to write a filter</h2>
            <div style={{ paddingLeft: '30px' }}>
                <p>
                    The query for the project grid is written in{' '}
                    <a
                        href="https://www.cloudbees.com/blog/yaml-tutorial-everything-you-need-get-started"
                        target="_blank"
                        rel="noreferrer"
                    >
                        YAML
                    </a>{' '}
                    and follows a specific structure. At the top level, there is a key for each
                    entity:
                </p>
                <ul>
                    <li>Family</li>
                    <li>Participant</li>
                    <li>Sample</li>
                    <li>Sequencing group</li>
                    <li>Assay</li>
                </ul>
                <p>Let&apos;s start with an example, this query searches the:</p>
                <p>
                    <ul>
                        <li>
                            <code>participant.externalId</code> for a value that contains
                            <code>&apos;participant-id&apos;</code>
                        </li>
                        <li>
                            <code>sample.meta.library</code> field for a value that starts with
                            <code>&apos;WGS&apos;</code>
                        </li>
                    </ul>
                </p>
                <DictEditor
                    input={exampleDictionary}
                    height="330px"
                    readonly
                />
                <h3>Query definition</h3>
                <p>
                    <em>Most</em> fields can be queried using the following operators:
                </p>
                <ul>
                    <li>
                        <code>eq</code> - Equals
                    </li>
                    <li>
                        <code>in_</code> - The value is in this list
                    </li>
                    <li>
                        <code>nin</code> - The value is not in this list
                    </li>
                    <li>
                        <code>gt</code> - Greater than
                    </li>
                    <li>
                        <code>gte</code> - Greater than or equal to
                    </li>
                    <li>
                        <code>lt</code> - Less than
                    </li>
                    <li>
                        <code>lte</code> - Less than or equal to
                    </li>
                    <li>
                        <code>contains</code> - Contains the value (case sensitive)
                    </li>
                    <li>
                        <code>icontains</code> - Contains the value (case insensitive)
                    </li>
                    <li>
                        <code>startswith</code> - Starts with the value (case sensitive)
                    </li>
                </ul>
                <h3>All fields to query</h3>
                <p>
                    This generated example shows you the fields available for querying from your
                    current search results. The meta fields to query vary based on the project, and
                    returned results. Refer to the example above for how these fields can be
                    queried.
                </p>
                <p>
                    Take note on the types of queries that can be used for each field. For example,
                    the sequencing group ID cannot be queried using the <code>contains</code>{' '}
                    operator.
                </p>

                <DictEditor
                    input={yamlString}
                    height="400px"
                    readonly
                />
            </div>
        </>
    )
}
