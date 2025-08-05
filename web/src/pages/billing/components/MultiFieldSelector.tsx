import * as React from 'react'
import { Message } from 'semantic-ui-react'
import {
    FormControl,
    InputLabel,
    Select,
    MenuItem,
    CircularProgress,
    SelectChangeEvent,
    OutlinedInput,
} from '@mui/material'
import { convertFieldName } from '../../../shared/utilities/fieldName'
import { BillingApi, BillingColumn, BillingTimePeriods } from '../../../sm-api'

interface MultiFieldSelectorProps {
    label: string
    fieldName: string
    selected?: string[]
    includeAll?: boolean
    autoSelect?: boolean
    onClickFunction: (
        event: SelectChangeEvent<string[]> | undefined,
        data: { value: string[] }
    ) => void
}

const MultiFieldSelector: React.FunctionComponent<MultiFieldSelectorProps> = ({
    label,
    fieldName,
    selected = [],
    includeAll,
    autoSelect,
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
        if (selected.length === 0 && autoSelect) {
            // set the first option as the default
            onClickFunction(undefined, { value: [extRecords[0]] })
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
    }, [fieldName])
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

    const handleSelectChange = (event: SelectChangeEvent<string[]>) => {
        const value = Array.isArray(event.target.value) ? event.target.value : [event.target.value]
        onClickFunction(event, { value })
    }

    const renderValue = (selectedValues: string[]) => {
        if (selectedValues.length === 0) {
            return `Select ${convertFieldName(fieldName)}`
        }
        if (selectedValues.length === 1) {
            const options = recordsMap(records as BillingColumn[])
            const option = options.find(opt => opt.value === selectedValues[0])
            return option?.text || selectedValues[0]
        }
        return `${selectedValues.length} ${convertFieldName(fieldName)}s selected`
    }

    if (error) {
        return (
            <Message negative>
                <h4>An error occurred while getting projects</h4>
                <p>{JSON.stringify(error)}</p>
            </Message>
        )
    }

    return (
        <FormControl fullWidth variant="outlined" disabled={loading}>
            <InputLabel id={`${fieldName}-multiselect-label`}>{label}</InputLabel>
            <Select
                labelId={`${fieldName}-multiselect-label`}
                id={`${fieldName}-multiselect`}
                multiple
                value={selected}
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
                {records && recordsMap(records as BillingColumn[]).map((option) => (
                    <MenuItem
                        key={option.key}
                        value={option.value}
                        sx={{
                            fontWeight: selected.includes(option.value) ? 600 : 400,
                            backgroundColor: selected.includes(option.value) ? 'rgba(25, 118, 210, 0.08)' : 'transparent',
                            '&:hover': {
                                backgroundColor: selected.includes(option.value)
                                    ? 'rgba(25, 118, 210, 0.12)'
                                    : 'rgba(0, 0, 0, 0.04)',
                            },
                            position: 'relative',
                            '&::after': selected.includes(option.value) ? {
                                content: '"✓"',
                                position: 'absolute',
                                right: '16px',
                                color: 'primary.main',
                                fontWeight: 'bold',
                            } : {},
                        }}
                    >
                        {option.text}
                    </MenuItem>
                ))}
            </Select>
        </FormControl>
    )
}

export default MultiFieldSelector
