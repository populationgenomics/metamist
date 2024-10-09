import React from 'react'
import { Dropdown, Form, Input, Label, Popup } from 'semantic-ui-react'
import {
    GenericFilterAny,
    ProjectParticipantGridField,
    ProjectParticipantGridFilter,
    ProjectParticipantGridFilterType,
} from '../../sm-api'

import CloseIcon from '@mui/icons-material/Close'
import FilterAltIcon from '@mui/icons-material/FilterAlt'
import FilterAltOutlinedIcon from '@mui/icons-material/FilterAltOutlined'
import { IconButton } from '@mui/material'

interface IValueFilter {
    filterValues: ProjectParticipantGridFilter
    updateFilterValues: (e: ProjectParticipantGridFilter) => void

    category: keyof ProjectParticipantGridFilter
    field: ProjectParticipantGridField
    position?: 'top right' | 'top center'

    size?: 'mini' | 'small' | 'large' | 'big' | 'huge' | 'massive'
}

const getQueryTypeFromOperator = (operator: string) => {
    switch (operator) {
        case 'startswith':
            return ProjectParticipantGridFilterType.Startswith
        case 'icontains':
            return ProjectParticipantGridFilterType.Icontains
        case 'eq':
            return ProjectParticipantGridFilterType.Eq
        case 'neq':
            return ProjectParticipantGridFilterType.Neq
        case 'gt':
            return ProjectParticipantGridFilterType.Gt
        case 'gte':
            return ProjectParticipantGridFilterType.Gte
        case 'lt':
            return ProjectParticipantGridFilterType.Lt
        case 'lte':
            return ProjectParticipantGridFilterType.Lte
    }
    return null
}

const getOperatorFromFilterType = (queryType: ProjectParticipantGridFilterType) => {
    return queryType
}

const getDisplayNameFromFilterType = (filterType: ProjectParticipantGridFilterType) => {
    switch (filterType) {
        case ProjectParticipantGridFilterType.Icontains:
            return 'Contains'
        case ProjectParticipantGridFilterType.Startswith:
            return 'Starts with'
        case ProjectParticipantGridFilterType.Eq:
            return 'Equals'
        case ProjectParticipantGridFilterType.Neq:
            return 'Does not equal'
        case ProjectParticipantGridFilterType.Gt:
            return 'Greater than'
        case ProjectParticipantGridFilterType.Gte:
            return 'Greater than or equal to'
        case ProjectParticipantGridFilterType.Lt:
            return 'Less than'
        case ProjectParticipantGridFilterType.Lte:
            return 'Less than or equal to'
    }
}

export const ValueFilter: React.FC<IValueFilter> = ({
    category,
    position,
    updateFilterValues,
    field,
    ...props
}) => {
    // Use the combination of category and filterKey to turn that into the correct value
    // Note filterValues is a ParticipantGridFilter object, which has nested keys for each
    // category.
    //
    // Nb: The server tells us the filterKey here, and for meta keys it is prefixed with 'meta.'
    //  So check if the filterKey starts with 'meta.' to determine if it is a meta key, and
    //  then check the [category].meta object for the value

    const [_defaultFilterType, setDefaultFilterType] = React.useState<
        ProjectParticipantGridFilterType | undefined
    >()

    let optionsToCheck = props?.filterValues?.[category] || {}
    const name = (field.filter_key ?? '').replace(/^meta\./, '')

    // @ts-ignore
    const _value = optionsToCheck?.[name]?.[operator]
    const [_tempValue, setTempValue] = React.useState<string | undefined>(_value ?? '')
    const tempValue = _tempValue ?? _value

    if (!field.filter_key) return <></>

    const isMeta = field.filter_key?.startsWith('meta.')
    // set name to the filterKey without the .meta prefix

    if (isMeta) {
        // get the meta bit from the filterValues
        optionsToCheck = optionsToCheck?.meta || {}
    }
    const isHighlighted = !!optionsToCheck && name in optionsToCheck

    // @ts-ignore
    const queryObj = optionsToCheck?.[name]

    // guess the operator from the queryObj in use, if there:
    //  - are multiple, disable the filter with the text "multiple filters set"
    //  - is an unsupported operator, disable the filter with the text "unsupported filter in this view"
    let disabled = false
    let queryType: ProjectParticipantGridFilterType | null = null
    let operator: string | null = null
    let message = ''
    // debugger
    if (queryObj !== undefined) {
        const operators = Object.keys(queryObj)
        if (operators.length > 1) {
            disabled = true
            message = 'multiple filters set'
        } else if (operators.length === 1) {
            const guessQType = getQueryTypeFromOperator(operators[0])
            if (guessQType) {
                queryType = guessQType
                operator = operators[0]
            } else {
                disabled = true
                message = 'unsupported filter in this view'
            }
        }
    }
    const options = field.filter_types ?? [
        ProjectParticipantGridFilterType.Icontains,
        ProjectParticipantGridFilterType.Startswith,
        ProjectParticipantGridFilterType.Eq,
        ProjectParticipantGridFilterType.Neq,
    ]
    if (!disabled && (!queryType || !operator)) {
        queryType = _defaultFilterType || options[0] || ProjectParticipantGridFilterType.Icontains
        operator = getOperatorFromFilterType(queryType)
    }

    const updateQueryType = (newFilterType: ProjectParticipantGridFilterType) => {
        setDefaultFilterType(newFilterType)
        const newOperator = getOperatorFromFilterType(newFilterType)
        if (!newOperator) return console.warn('No operator from query type', newFilterType)
        if (tempValue) postValue(newOperator, tempValue)
    }

    const postValue = (_operator: string, value: string | undefined) => {
        const f: GenericFilterAny | undefined = value ? { [_operator]: value } : undefined

        // deep copy
        const newFilter = JSON.parse(JSON.stringify(props.filterValues))

        const base = newFilter[category] || {}
        if (isMeta) {
            const oldMeta = base.meta || {}
            newFilter[category] = { ...base, meta: { ...oldMeta, [name]: f } }
        } else {
            newFilter[category] = { ...base, [name]: f }
        }

        updateFilterValues(newFilter)
    }

    const onSubmit = (e: React.FormEvent<HTMLFormElement>) => {
        e.preventDefault()
        postValue(operator!, tempValue)
    }

    let input: React.ReactElement = <p>{message}</p>

    if (!disabled) {
        input = (
            <Input
                icon="search"
                placeholder={`Filter ${name}...`}
                name={name}
                value={tempValue}
                onChange={(e) => setTempValue(e.target.value)}
                size={props.size}
                labelPosition="left"
                onSubmit={onSubmit}
            >
                {/* label={ */}
                <Label>
                    <Dropdown
                        value={queryType || ''}
                        onChange={(_, { value }) =>
                            updateQueryType(value as ProjectParticipantGridFilterType)
                        }
                        options={options.map((q) => ({
                            key: q,
                            text: getDisplayNameFromFilterType(q),
                            value: q,
                        }))}
                    />
                </Label>
                <input style={{ width: 'auto' }} />
                {isHighlighted && (
                    <Label style={{}}>
                        <IconButton
                            style={{ padding: '0', height: '12px', width: '12px' }}
                            onClick={() => {
                                if (!operator) {
                                    return alert('No operator, please report this issue.')
                                }
                                postValue(operator, undefined)
                                setTempValue('')
                            }}
                        >
                            <CloseIcon style={{ padding: '0', margin: '0' }} />
                        </IconButton>
                    </Label>
                )}

                {/* } */}
            </Input>
        )
    }
    // debugger

    return (
        <Form onSubmit={onSubmit}>
            <Form.Group inline style={{ padding: 0, margin: 0 }}>
                {input}
            </Form.Group>
        </Form>
    )
}

export const ValueFilterPopup: React.FC<IValueFilter> = (props) => {
    if (!props.field.filter_key) return <>No key</>
    const isMeta = props.field.filter_key?.startsWith('meta.')
    // set name to the filterKey without the .meta prefix
    const name = props.field.filter_key.replace(/^meta\./, '')

    let optionsToCheck = props?.filterValues?.[props.category] || {}

    if (isMeta) {
        // get the meta bit from the filterValues
        optionsToCheck = optionsToCheck?.meta || {}
    }
    const isHighlighted = !!optionsToCheck && name in optionsToCheck

    return (
        <div style={{ position: 'relative' }}>
            <div style={{ position: 'absolute', top: 0, right: 0 }}>
                <Popup
                    on="click"
                    position={props.position || 'top right'}
                    trigger={isHighlighted ? <FilterAltIcon /> : <FilterAltOutlinedIcon />}
                    hoverable
                >
                    <ValueFilter {...props} />
                </Popup>
            </div>
        </div>
    )
}
