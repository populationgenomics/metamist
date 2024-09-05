import RobotIcon from '@mui/icons-material/SmartToy'
import { Avatar, SxProps } from '@mui/material'

const colorFromStr = (str: string) => {
    let hash = 0
    if (str.length === 0) return `hsl(0 0 50)`
    for (let i = 0; i < str.length; i++) {
        hash = str.charCodeAt(i) + ((hash << 5) - hash)
        hash = hash & hash
    }
    return `hsl(${hash % 360} 30 50)`
}

export function CommentAvatar(props: {
    username: string
    isMachineUser: boolean
    name: string
    initials: string
    size: 'full' | 'small'
}) {
    const { isMachineUser, username, initials, name, size } = props
    const styleProps: SxProps =
        size === 'full'
            ? { width: 32, height: 32, fontSize: 14 }
            : { width: 28, height: 28, fontSize: 8 }
    return (
        <Avatar
            sx={{ bgcolor: colorFromStr(username), fontWeight: 'bold', ...styleProps }}
            alt={name}
        >
            {isMachineUser ? <RobotIcon /> : initials}
        </Avatar>
    )
}
