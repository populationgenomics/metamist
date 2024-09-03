import { Box } from '@mui/material'
import React, { PointerEventHandler, useRef, useState } from 'react'
export type PanelProps = {
    width: number
}

export type SplitPageProps = {
    main: (props: PanelProps) => React.ReactNode
    side: (props: PanelProps) => React.ReactNode
}

const DOUBLE_CLICK_TIME = 100

type DragState = {
    isDragging: boolean
    percent: number
}

export function SplitPage(props: SplitPageProps) {
    const divider = useRef<HTMLDivElement>(null)
    const container = useRef<HTMLDivElement>(null)
    const [clickTime, setClickTime] = useState<number | null>(null)
    const [dragState, setDragState] = useState<DragState>({ isDragging: false, percent: 70 })

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

        setDragState({
            isDragging: true,
            percent,
        })
    }

    const handlePointerUp: PointerEventHandler = (e) => {
        // console.log('up', e)
        const onDivider = e.target === divider.current
        setDragState({ isDragging: false, percent: dragState.percent })

        if (onDivider) {
            const isDoubleClick = clickTime && Date.now() - clickTime < DOUBLE_CLICK_TIME
            // toggle sidebar
            if (isDoubleClick) {
                setClickTime(null)
            }
        }
    }

    const handlePointerLeave: PointerEventHandler = () => {
        if (dragState.isDragging) {
            setDragState({ isDragging: false, percent: dragState.percent })
        }
    }

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
            <Box style={{ width: `${dragState.percent}%` }} sx={{ overflowY: 'auto' }}>
                {props.main({ width: 10 })}
            </Box>

            <Box
                style={{ width: `${100 - dragState.percent}%` }}
                position={'fixed'}
                height={'100%'}
                top={0}
                right={0}
                pl={'4px'}
                pt={7}
                zIndex={1}
            >
                <div
                    ref={divider}
                    style={{
                        width: '4px',
                        height: '100%',
                        background: 'var(--color-border-color)',
                        position: 'absolute',
                        left: 0,
                        cursor: 'col-resize',
                    }}
                ></div>
                <Box height={'100vh'} sx={{ overflowY: 'auto' }}>
                    {props.side({ width: 10 })}
                </Box>
            </Box>
        </Box>
    )
}
