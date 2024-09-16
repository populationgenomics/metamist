import { Box } from '@mui/material'
import { ComponentType, memo, PointerEventHandler, useCallback, useRef, useState } from 'react'
export type PanelProps = {
    collapsed: boolean
    onToggleCollapsed: (collapsed: boolean) => void
}

export type SplitPageProps = {
    main: ComponentType<PanelProps>
    side: ComponentType<PanelProps>
    collapsed: boolean
    collapsedWidth: number
}

// The amount of time between pointer down and up to consider being a click
const CLICK_DURATION = 150

type DragState = {
    isDragging: boolean
    percent: number
}

// Split the panel rendering out into a separate component so it can be memoized
const SplitPagePanel = memo(function SplitPagePanel(
    props: { component: ComponentType<PanelProps> } & PanelProps
) {
    const Panel = props.component
    return <Panel collapsed={props.collapsed} onToggleCollapsed={props.onToggleCollapsed} />
})

export function SplitPage(props: SplitPageProps) {
    const divider = useRef<HTMLDivElement>(null)
    const container = useRef<HTMLDivElement>(null)
    const [clickTime, setClickTime] = useState<number | null>(null)
    const [dragState, setDragState] = useState<DragState>({ isDragging: false, percent: 70 })
    const [collapsed, setCollapsed] = useState(props.collapsed)

    const handlePointerDown: PointerEventHandler = (e) => {
        const onDivider = e.target === divider.current
        if (onDivider) {
            e.preventDefault()
            setDragState({ isDragging: true, percent: dragState.percent })
            setClickTime(Date.now())
        }
    }
    const handlePointerMove: PointerEventHandler = (e) => {
        if (!container.current || !dragState.isDragging) return true

        const cX = e.clientX

        const containerBox = container.current.getBoundingClientRect()
        const containerWidth = containerBox.width
        const containerLeft = containerBox.left

        const percent = ((cX - containerLeft) / containerWidth) * 100
        setCollapsed(false)
        setDragState({
            isDragging: true,
            percent,
        })
    }

    const handlePointerUp: PointerEventHandler = (e) => {
        const onDivider = e.target === divider.current
        setDragState({ isDragging: false, percent: dragState.percent })

        if (onDivider) {
            const isClick = clickTime && Date.now() - clickTime < CLICK_DURATION
            if (isClick) {
                if (collapsed) {
                    setDragState({
                        isDragging: false,
                        // Expand out to at least 30%
                        percent: Math.min(dragState.percent, 70),
                    })
                }
                setCollapsed(!collapsed)
                setClickTime(null)
            }
        }
    }

    const handlePointerLeave: PointerEventHandler = () => {
        if (dragState.isDragging) {
            setDragState({ isDragging: false, percent: dragState.percent })
        }
    }

    const mainWidth = collapsed ? `calc(100% - ${props.collapsedWidth}px)` : `${dragState.percent}%`
    const sideWidth = collapsed ? `${props.collapsedWidth}px` : `${100 - dragState.percent}%`

    const onToggleCollapsed = useCallback(
        (newCollapsed: boolean) => {
            if (collapsed) {
                setDragState((dragState) => ({
                    isDragging: false,
                    // Expand out to at least 30%
                    percent: Math.min(dragState.percent, 70),
                }))
            }
            setCollapsed(newCollapsed)
        },
        [collapsed]
    )

    return (
        <Box
            ref={container}
            width="100%"
            maxWidth="100%"
            onPointerDown={handlePointerDown}
            onPointerMove={handlePointerMove}
            onPointerUp={handlePointerUp}
            onPointerLeave={handlePointerLeave}
            style={{
                ...(dragState.isDragging ? { cursor: 'col-resize' } : {}),
            }}
        >
            <Box style={{ width: mainWidth }} sx={{ overflowY: 'auto' }}>
                <SplitPagePanel
                    component={props.main}
                    collapsed={collapsed}
                    onToggleCollapsed={onToggleCollapsed}
                />
            </Box>

            <Box
                style={{ width: sideWidth }}
                position={'fixed'}
                bottom={0}
                top={56}
                right={0}
                zIndex={1}
            >
                <Box
                    ref={divider}
                    sx={{
                        width: '4px',
                        height: '100%',
                        background: 'var(--color-border-color)',
                        position: 'absolute',
                        left: 0,
                        cursor: 'col-resize',
                        zIndex: 10,
                        '&:hover': {
                            background: 'var(--color-text-href)',
                        },
                    }}
                    style={{
                        ...(dragState.isDragging ? { background: 'var(--color-text-href)' } : {}),
                    }}
                ></Box>
                <Box position={'absolute'} height={'100%'} left={4} right={0}>
                    <SplitPagePanel
                        component={props.side}
                        collapsed={collapsed}
                        onToggleCollapsed={onToggleCollapsed}
                    />
                </Box>
            </Box>
        </Box>
    )
}
