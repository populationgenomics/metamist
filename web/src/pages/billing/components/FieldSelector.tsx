import {
    CircularProgress,
    Divider,
    FormControl,
    InputLabel,
    MenuItem,
    OutlinedInput,
    Select,
    SelectChangeEvent,
} from '@mui/material'
import * as React from 'react'
import { Message } from 'semantic-ui-react'
import { convertFieldName } from '../../../shared/utilities/fieldName'
import { BillingApi, BillingColumn, BillingTimePeriods } from '../../../sm-api'

interface FieldSelectorProps {
    label: string
    fieldName: string
    selected?: string | string[]
    includeAll?: boolean
    autoSelect?: boolean
    multiple?: boolean
    preloadedData?: string[]
    onClickFunction: (
        event: SelectChangeEvent<string | string[]> | undefined,
        data: { value: string | string[] }
    ) => void
}

const FieldSelector: React.FunctionComponent<FieldSelectorProps> = ({
    label,
    fieldName,
    selected,
    includeAll,
    autoSelect,
    multiple = false,
    preloadedData,
    onClickFunction,
}) => {
    const [loading, setLoading] = React.useState<boolean>(true)
    const [error, setError] = React.useState<string | undefined>()
    const [records, setRecords] = React.useState<string[]>([])

    const extendRecords = (recs: string[]) => {
        if (includeAll) {
            let fname = convertFieldName(fieldName)
            if (fname.endsWith('y')) {
                fname = fname.substring(0, fname.length - 1) + 'ie'
            }
            return [`All ${fname}s`, ...recs]
        }
        return recs
    }

    const processResponse = (response_data: string[]) => {
        setLoading(false)
        const extRecords = extendRecords(response_data)
        setRecords(extRecords)
        if (!selected && autoSelect) {
            if (multiple) {
                onClickFunction(undefined, { value: [extRecords[0]] })
            } else {
                onClickFunction(undefined, { value: extRecords[0] })
            }
        }
    }

    const getTopics = () => {
        setLoading(true)
        setError(undefined)
        new BillingApi()
            .getTopics()
            .then((response) => {
                processResponse(response.data)
            })
            .catch((er) => setError(er.message))
    }

    const getGcpProjects = () => {
        setLoading(true)
        setError(undefined)
        new BillingApi()
            .getGcpProjects()
            .then((response) => {
                processResponse(response.data)
            })
            .catch((er) => setError(er.message))
    }

    const getInvoiceMonths = () => {
        setLoading(true)
        setError(undefined)
        new BillingApi()
            .getInvoiceMonths()
            .then((response) => {
                processResponse(response.data)
            })
            .catch((er) => setError(er.message))
    }

    const getStages = () => {
        setLoading(true)
        setError(undefined)
        new BillingApi()
            .getStages()
            .then((response) => {
                processResponse(response.data)
            })
            .catch((er) => setError(er.message))
    }

    const getCostCategories = () => {
        setLoading(true)
        setError(undefined)
        new BillingApi()
            .getCostCategories()
            .then((response) => {
                processResponse(response.data)
            })
            .catch((er) => setError(er.message))
    }

    /* eslint-disable react-hooks/exhaustive-deps -- this is missing a ton of deps, probably buggy but hard to fix */
    React.useEffect(() => {
        // If preloaded data is available, use it instead of making API calls
        if (preloadedData && preloadedData.length > 0) {
            setRecords(preloadedData)
            setLoading(false)
            return
        }

        // Otherwise, proceed with normal API fetching
        if (fieldName === BillingColumn.Topic) getTopics()
        else if (fieldName === BillingColumn.GcpProject) getGcpProjects()
        else if (fieldName === BillingColumn.InvoiceMonth) getInvoiceMonths()
        else if (fieldName === BillingColumn.Stage) getStages()
        else if (fieldName === BillingColumn.CostCategory) getCostCategories()
        else if (fieldName === 'Group') {
            setRecords([BillingColumn.GcpProject, BillingColumn.Topic, BillingColumn.Stage])
            setLoading(false)
        } else if (fieldName === 'Period') {
            setRecords([
                BillingTimePeriods.Day,
                BillingTimePeriods.Week,
                BillingTimePeriods.Month,
                BillingTimePeriods.InvoiceMonth,
            ])
            setLoading(false)
        } else {
            setError(`Could not load records for ${fieldName}`)
        }
    }, [fieldName, preloadedData])
    /* eslint-enable react-hooks/exhaustive-deps */

    const formatInvoiceMonth = (invoiceMonth: string): string => {
        // Convert from format like "202505" to "May 2025"
        if (invoiceMonth.length === 6) {
            const year = invoiceMonth.substring(0, 4)
            const month = invoiceMonth.substring(4, 6)
            const date = new Date(parseInt(year), parseInt(month) - 1, 1)
            return date.toLocaleDateString('en-US', {
                year: 'numeric',
                month: 'long',
            })
        }
        return invoiceMonth
    }

    const capitalize = (str: string): string => {
        if (str === 'gcp_project') {
            return 'GCP-Project'
        }
        return str.charAt(0).toUpperCase() + str.slice(1)
    }

    const recordsMap = (records: BillingColumn[]) => {
        if (fieldName === 'Group') {
            return records.map((p: BillingColumn) => ({
                key: p,
                text: capitalize(p),
                value: p,
            }))
        }

        // Special formatting for invoice months
        if (fieldName === BillingColumn.InvoiceMonth) {
            return records.map((p: string) => ({
                key: p,
                text: formatInvoiceMonth(p),
                value: p,
            }))
        }

        return records.map((p: string) => ({
            key: p,
            text: p,
            value: p,
        }))
    }

    const getSortedOptions = (options: { key: string; text: string; value: string }[]) => {
        if (!multiple || !Array.isArray(selected)) return options

        // Sort options to show selected items at the top, maintaining original order within each group
        const selectedOptions = options.filter((option) => selected.includes(option.value))
        const unselectedOptions = options.filter((option) => !selected.includes(option.value))

        return [...selectedOptions, ...unselectedOptions]
    }

    const handleSelectChange = (event: SelectChangeEvent<string | string[]>) => {
        let value: string | string[]

        if (multiple) {
            value = Array.isArray(event.target.value) ? event.target.value : [event.target.value]
        } else {
            value = Array.isArray(event.target.value) ? event.target.value[0] : event.target.value
        }

        onClickFunction(event, { value })
    }

    const renderValue = (selectedValues: string | string[]) => {
        if (multiple && Array.isArray(selectedValues)) {
            if (selectedValues.length === 0) {
                return `Select ${convertFieldName(fieldName)}`
            }
            if (selectedValues.length === 1) {
                const options = recordsMap(records as BillingColumn[])
                const option = options.find((opt) => opt.value === selectedValues[0])
                return option?.text || selectedValues[0]
            }
            return `${selectedValues.length} ${convertFieldName(fieldName)}s selected`
        }

        if (!multiple && typeof selectedValues === 'string') {
            if (!selectedValues) {
                return `Select ${convertFieldName(fieldName)}`
            }
            const options = recordsMap(records as BillingColumn[])
            const option = options.find((opt) => opt.value === selectedValues)
            return option?.text || selectedValues
        }

        return `Select ${convertFieldName(fieldName)}`
    }

    if (error) {
        return (
            <Message negative>
                <h4>An error occurred while getting projects</h4>
                <p>{JSON.stringify(error)}</p>
            </Message>
        )
    }

    const options = recordsMap(records as BillingColumn[])
    const sortedOptions = getSortedOptions(options)

    // Ensure selected values exist in available options to avoid warnings
    const availableValues = options.map((opt) => opt.value)

    const selectedValue = multiple
        ? Array.isArray(selected)
            ? selected.filter((val) => availableValues.includes(val))
            : []
        : Array.isArray(selected)
          ? availableValues.includes(selected[0])
              ? selected[0]
              : ''
          : availableValues.includes(selected || '')
            ? selected || ''
            : ''

    return (
        <FormControl fullWidth variant="outlined" disabled={loading}>
            <InputLabel id={`${fieldName}-select-label`}>{label}</InputLabel>
            <Select
                labelId={`${fieldName}-select-label`}
                id={`${fieldName}-select`}
                multiple={multiple}
                value={selectedValue}
                onChange={handleSelectChange}
                input={<OutlinedInput label={label} />}
                renderValue={renderValue}
                startAdornment={loading ? <CircularProgress size={20} /> : null}
                MenuProps={{
                    PaperProps: {
                        style: {
                            maxHeight: 300,
                        },
                    },
                }}
            >
                {records &&
                    (() => {
                        const elements: React.ReactNode[] = []
                        const selectedCount =
                            multiple && Array.isArray(selected) ? selected.length : 0

                        sortedOptions.forEach((option, index) => {
                            const isSelected = multiple
                                ? Array.isArray(selected) && selected.includes(option.value)
                                : selected === option.value

                            const shouldShowDivider =
                                multiple &&
                                index === selectedCount &&
                                selectedCount > 0 &&
                                selectedCount < options.length

                            if (shouldShowDivider) {
                                elements.push(<Divider key={`divider-${index}`} sx={{ my: 0.5 }} />)
                            }

                            elements.push(
                                <MenuItem
                                    key={option.key}
                                    value={option.value}
                                    sx={{
                                        fontWeight: isSelected ? 600 : 400,
                                        backgroundColor: isSelected
                                            ? 'rgba(25, 118, 210, 0.08)'
                                            : 'transparent',
                                        '&:hover': {
                                            backgroundColor: isSelected
                                                ? 'rgba(25, 118, 210, 0.12)'
                                                : 'rgba(0, 0, 0, 0.04)',
                                        },
                                        position: 'relative',
                                        '&::after':
                                            multiple && isSelected
                                                ? {
                                                      content: '"✓"',
                                                      position: 'absolute',
                                                      right: '16px',
                                                      color: 'primary.main',
                                                      fontWeight: 'bold',
                                                  }
                                                : {},
                                    }}
                                >
                                    {option.text}
                                </MenuItem>
                            )
                        })

                        return elements
                    })()}
            </Select>
        </FormControl>
    )
}

export default FieldSelector
