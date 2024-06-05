import React from 'react'
import { Form, Popup } from 'semantic-ui-react'
import { GenericFilterAny, ProjectParticipantGridFilter } from '../../sm-api'

import CloseIcon from '@mui/icons-material/Close'
import FilterAltIcon from '@mui/icons-material/FilterAlt'
import FilterAltOutlinedIcon from '@mui/icons-material/FilterAltOutlined'
import { IconButton } from '@mui/material'

interface IValueFilter {
    filterValues: ProjectParticipantGridFilter
    updateFilterValues: (e: Partial<ProjectParticipantGridFilter>) => void

    category: keyof ProjectParticipantGridFilter
    filterKey: string
    position?: 'top right' | 'top center'

    size?: 'mini' | 'small' | 'large' | 'big' | 'huge' | 'massive'
}
export const ValueFilter: React.FC<IValueFilter> = ({
    category,
    position,
    updateFilterValues,
    ...props
}) => {
    // Use the combination of category and filterKey to turn that into the correct value
    // Note filterValues is a ParticipantGridFilter object, which has nested keys for each
    // category (except participant).
    //
    // Nb: The server tells us the filterKey here, and for meta keys it is prefixed with 'meta.'
    //  So check if the filterKey starts with 'meta.' to determine if it is a meta key, and
    //  then check the [category].meta object for the value

    if (!props.filterKey) return <>No key</>
    const isMeta = props.filterKey?.startsWith('meta.')
    // set name to the filterKey without the .meta prefix
    const name = props.filterKey.replace(/^meta\./, '')

    // if we are filtering on the participant level, check the filterValues directly
    let optionsToCheck = props?.filterValues?.[category] || {}

    if (isMeta) {
        // get the meta bit from the filterValues
        optionsToCheck = optionsToCheck?.meta || {}
    }
    const isHighlighted = !!optionsToCheck && name in optionsToCheck

    // TODO: remove this type ignore
    // @ts-ignore
    const _value = optionsToCheck?.[name]?.icontains
    const [_tempValue, setTempValue] = React.useState<string | undefined>(_value ?? '')
    const tempValue = _tempValue ?? _value

    const postValue = (value: string | undefined) => {
        const f: GenericFilterAny = { icontains: value }

        const update: ProjectParticipantGridFilter = {
            [category]: !isMeta
                ? { name: f }
                : {
                      meta: { [name]: f },
                  },
        }

        updateFilterValues(update)
    }

    const onSubmit = (e: React.FormEvent<HTMLFormElement>) => {
        e.preventDefault()
        postValue(tempValue)
    }

    return (
        <Form onSubmit={onSubmit}>
            <Form.Group inline style={{ padding: 0, margin: 0 }}>
                <Form.Field style={{ padding: 0, margin: 0 }}>
                    <Form.Input
                        action={{ icon: 'search' }}
                        placeholder={`Filter ${name}...`}
                        name={name}
                        value={tempValue}
                        onChange={(e) => setTempValue(e.target.value)}
                        size={props.size}
                    />
                </Form.Field>
                {isHighlighted && (
                    <Form.Field style={{ padding: 0 }}>
                        <IconButton
                            onClick={() => {
                                setTempValue('')
                                postValue(undefined)
                            }}
                            style={{ padding: 0 }}
                        >
                            <CloseIcon />
                        </IconButton>
                    </Form.Field>
                )}
            </Form.Group>
        </Form>
    )
}

export const ValueFilterPopup: React.FC<IValueFilter> = (props) => {
    if (!props.filterKey) return <>No key</>
    const isMeta = props.filterKey?.startsWith('meta.')
    // set name to the filterKey without the .meta prefix
    const name = props.filterKey.replace(/^meta\./, '')

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
