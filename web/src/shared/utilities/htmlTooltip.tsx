import Tooltip, { TooltipProps } from '@mui/material/Tooltip'

const HtmlTooltip = (props: TooltipProps) => (
    <Tooltip {...props} classes={{ popper: 'html-tooltip' }} />
)

export default HtmlTooltip
