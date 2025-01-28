import { Box, Card, CardActions, CardContent, Typography } from '@mui/material'
import { formatQuery } from '../data/formatQuery'
import { MetricFromQuery, MetricProps } from './MetricFromQuery'
import { PlotFromQuery, PlotProps } from './PlotFromQuery'
import {
    ActionViewEditSql,
    ActionViewExpandedPlot,
    ActionViewExpandedTable,
} from './ReportItemActions'
import { TableFromQuery, TableProps } from './TableFromQuery'

type BaseReportItemProps = {
    title?: string
    subtitle?: string
    description?: string
    height: number
    flexBasis?: number | string
    flexGrow: number
    flexShrink?: number
    minWidth?: number | string
    maxWidth?: number | string
}

type ReportItemContentProps = {
    content: React.ReactNode
    actions: React.ReactNode
}

export function ReportItem(props: BaseReportItemProps & ReportItemContentProps) {
    const { flexBasis, flexGrow, flexShrink, minWidth, maxWidth } = props
    return (
        <Card
            sx={{
                position: 'relative',
                display: 'flex',
                height: props.height,
                flexBasis,
                flexGrow,
                flexShrink,
                minWidth,
                maxWidth,
            }}
        >
            <CardActions
                disableSpacing
                sx={{
                    position: 'absolute',
                    right: 0,
                }}
            >
                {props.actions}
            </CardActions>
            <CardContent
                sx={{
                    display: 'flex',
                    gap: 2,
                    flexDirection: 'column',
                    flexGrow: 1,
                    height: '100%',
                    contain: 'size',
                }}
            >
                {(props.title || props.subtitle) && (
                    <Box>
                        {props.title && (
                            <Typography fontWeight={'bold'} fontSize={16}>
                                {props.title}
                            </Typography>
                        )}
                        {props.subtitle && <Typography fontSize={14}>{props.subtitle}</Typography>}
                    </Box>
                )}
                <Box
                    flexGrow={1}
                    sx={{
                        position: 'relative',
                        // This is a bit of a lesser known css property.
                        // setting contain to 'size' will sever the link between the container
                        // and the content in it, meaning that the container will no longer grow
                        // with the child content. This is useful for this sort of dashboard layout
                        // where we want the content of the card to fill up the space left over by
                        // the title, subtitle and description. This ensures nice alignment of cards
                        // with the same height specified.
                        contain: 'size',
                    }}
                >
                    {props.content}
                </Box>
                {props.description && (
                    <Box width={'100%'}>
                        <Typography fontSize={12}>{props.description}</Typography>
                    </Box>
                )}
            </CardContent>
        </Card>
    )
}

type ReportItemPlotProps = BaseReportItemProps & PlotProps

export function ReportItemPlot(props: ReportItemPlotProps) {
    const { project, query: unformattedQuery, plot, ...reportItemProps } = props
    const query = formatQuery(unformattedQuery)

    return (
        <ReportItem
            content={<PlotFromQuery project={props.project} query={query} plot={props.plot} />}
            actions={
                <Box>
                    <ActionViewEditSql project={props.project} query={query} />
                    <ActionViewExpandedPlot
                        project={props.project}
                        query={query}
                        plot={props.plot}
                    />
                    <ActionViewExpandedTable project={props.project} query={query} />
                </Box>
            }
            {...reportItemProps}
        />
    )
}

type ReportItemTableProps = BaseReportItemProps & TableProps

export function ReportItemTable(props: ReportItemTableProps) {
    const { project, query: unformattedQuery, showToolbar, ...reportItemProps } = props
    const query = formatQuery(unformattedQuery)

    return (
        <ReportItem
            content={
                <Box
                    display={'flex'}
                    flexDirection={'column'}
                    position={'absolute'}
                    height={'100%'}
                    width={'100%'}
                >
                    <TableFromQuery
                        project={props.project}
                        query={query}
                        showToolbar={props.showToolbar}
                    />
                </Box>
            }
            actions={
                <Box>
                    <ActionViewEditSql project={props.project} query={query} />
                    <ActionViewExpandedTable project={props.project} query={query} />
                </Box>
            }
            {...reportItemProps}
        />
    )
}

type ReportItemMetricProps = BaseReportItemProps & MetricProps

export function ReportItemMetric(props: ReportItemMetricProps) {
    const { project, query: unformattedQuery, ...reportItemProps } = props
    const query = formatQuery(unformattedQuery)

    return (
        <ReportItem
            content={
                <Box>
                    <MetricFromQuery project={props.project} query={query} />
                </Box>
            }
            actions={
                <Box>
                    <ActionViewEditSql project={props.project} query={query} />
                    <ActionViewExpandedTable project={props.project} query={query} />
                </Box>
            }
            {...reportItemProps}
        />
    )
}
