import { SvgIcon, SvgIconProps } from '@mui/material'

export default function AssayIcon(props: SvgIconProps) {
    return (
        <SvgIcon {...props}>
            <svg
                xmlns="http://www.w3.org/2000/svg"
                height="24px"
                viewBox="0 -960 960 960"
                width="24px"
            >
                <path d="M480-60q-63 0-106.5-43.5T330-210q0-52 31-91.5t79-53.5v-85H200v-160H100v-280h280v280H280v80h400v-85q-48-14-79-53.5T570-750q0-63 43.5-106.5T720-900q63 0 106.5 43.5T870-750q0 52-31 91.5T760-605v165H520v85q48 14 79 53.5t31 91.5q0 63-43.5 106.5T480-60Zm240-620q29 0 49.5-20.5T790-750q0-29-20.5-49.5T720-820q-29 0-49.5 20.5T650-750q0 29 20.5 49.5T720-680Zm-540 0h120v-120H180v120Zm300 540q29 0 49.5-20.5T550-210q0-29-20.5-49.5T480-280q-29 0-49.5 20.5T410-210q0 29 20.5 49.5T480-140ZM240-740Zm480-10ZM480-210Z" />
            </svg>
        </SvgIcon>
    )
}
