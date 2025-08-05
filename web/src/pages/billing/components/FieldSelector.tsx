import {
    CircularProgress,
    FormControl,
    InputLabel,
    MenuItem,
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
    selected?: string
    includeAll?: boolean
    autoSelect?: boolean
    onClickFunction: (event: SelectChangeEvent<string> | undefined, data: { value: string }) => void
}

const FieldSelector: React.FunctionComponent<FieldSelectorProps> = ({
    label,
    fieldName,
    selected,
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
        if (!selected && autoSelect) {
            // set the first option as the default
            onClickFunction(undefined, { value: extRecords[0] })
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

    const handleSelectChange = (event: SelectChangeEvent<string>) => {
        onClickFunction(event, { value: event.target.value })
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
            <InputLabel id={`${fieldName}-select-label`}>{label}</InputLabel>
            <Select
                labelId={`${fieldName}-select-label`}
                id={`${fieldName}-select`}
                value={selected ?? ''}
                onChange={handleSelectChange}
                label={label}
                startAdornment={loading ? <CircularProgress size={20} /> : null}
            >
                {records &&
                    recordsMap(records as BillingColumn[]).map((option) => (
                        <MenuItem key={option.key} value={option.value}>
                            {option.text}
                        </MenuItem>
                    ))}
            </Select>
        </FormControl>
    )
}

export default FieldSelector
