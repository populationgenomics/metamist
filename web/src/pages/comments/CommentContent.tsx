import { memo } from 'react'
import Markdown from 'react-markdown'
import { Prism as SyntaxHighlighter } from 'react-syntax-highlighter'
import { materialDark, materialLight } from 'react-syntax-highlighter/dist/esm/styles/prism'
import remarkGfm from 'remark-gfm'

export const CommentContent = memo(function CommentContent(props: {
    content: string | undefined
    theme: 'light' | 'dark'
}) {
    const content = props.content || ''
    return (
        <Markdown
            remarkPlugins={[remarkGfm]}
            components={{
                code({ node, inline, className, children, ...props_ }) {
                    const match_ = /language-(\w+)/.exec(className || '')
                    return !inline && match_ ? (
                        <SyntaxHighlighter
                            language={match_[1]}
                            style={props.theme === 'dark' ? materialDark : materialLight}
                            PreTag="div"
                            CodeTag="div"
                        >
                            {children}
                        </SyntaxHighlighter>
                    ) : (
                        <code className={className} {...props_}>
                            {children}
                        </code>
                    )
                },
            }}
        >
            {content}
        </Markdown>
    )
})
