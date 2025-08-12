import { Alert, Autocomplete, Box, Button, Chip, ChipProps, TextField } from '@mui/material'
import { useState } from 'react'

const MAX_IDS_TO_SHOW = 250

type IdSelectorChipProps = { idPrefixes: string[]; value: string } & ChipProps

function IdSelectorChip(props: IdSelectorChipProps) {
    const { idPrefixes, value, ...chipProps } = props

    const validEntry = idPrefixes.some((prefix) => value.startsWith(prefix))

    return (
        <Chip
            {...chipProps}
            label={value}
            size={'small'}
            color={validEntry ? 'primary' : 'error'}
        />
    )
}

type IdSelectorProps = {
    idPrefixes: string[]
    idList: string[]
    onChange: (value: string[]) => void
}

const cleanId = (id: string) => {
    return id
        .replace(/[^\dA-z]/g, '')
        .trim()
        .toUpperCase()
}

export default function IdSelector(props: IdSelectorProps) {
    const { onChange, idList, idPrefixes } = props
    const [selectedIdInput, setSelectedIdInput] = useState<string>('')

    // Handle changes to the value typed or entered by the user in the input box
    // This allows us to update the id list once multiple ids are entered
    function handleInputChange(value: string) {
        // Just grab the part of the input up until the final separator, so that we don't
        // turn what the user is currently writing into a chip
        const valueToParseMatch = value.match(/^.*[\s,]/)
        if (!valueToParseMatch || valueToParseMatch.length === 0) {
            setSelectedIdInput(value)
            return
        }

        const valueToParse = valueToParseMatch[0]

        const ids = valueToParse
            .split(/[,\s]+/)
            .map(cleanId)
            .filter(Boolean)

        const remainingValue = value.replace(/^.*[\s,]/, '')

        setSelectedIdInput(remainingValue)
        onChange(idList.concat(ids))
    }

    function handleInputBlur() {
        if (selectedIdInput) {
            setSelectedIdInput('')
            onChange(idList.concat([cleanId(selectedIdInput)]))
        }
    }

    const invalidIds = idList.filter((id) => !idPrefixes.some((prefix) => id.startsWith(prefix)))

    return (
        <Box>
            <Box display="flex">
                <Autocomplete
                    clearIcon={false}
                    options={[]}
                    value={idList}
                    onChange={(_e, value) => onChange(value.map(cleanId))}
                    inputValue={selectedIdInput}
                    onInputChange={(_e, value) => handleInputChange(value)}
                    onBlur={() => handleInputBlur()}
                    freeSolo
                    fullWidth
                    multiple
                    renderTags={(value, props) =>
                        value
                            // Limit how many ids we try to render
                            // get them from the end so newly added ids don't disappear
                            .map((option, index) => ({ option, index }))
                            .slice(-1 * MAX_IDS_TO_SHOW)
                            .map(({ option, index }) => (
                                <IdSelectorChip
                                    idPrefixes={idPrefixes}
                                    value={option}
                                    {...props({ index: index })}
                                    key={option}
                                />
                            ))
                    }
                    renderInput={(params) => (
                        <TextField
                            label="Sample or Sequencing Group IDs"
                            helperText="Enter a comma-separated list of IDs"
                            {...params}
                        />
                    )}
                />
                <Box pt={1.5} ml={2}>
                    <Button
                        size="small"
                        onClick={() => {
                            onChange([])
                            setSelectedIdInput('')
                        }}
                    >
                        Clear All
                    </Button>
                </Box>
            </Box>
            {invalidIds.length > 0 && (
                <Alert severity="error">ID list contains {invalidIds.length} invalid ids</Alert>
            )}
        </Box>
    )
}
