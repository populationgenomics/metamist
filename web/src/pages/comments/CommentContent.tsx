import { memo } from 'react'
import Markdown from 'react-markdown'
import SyntaxHighlighter from 'react-syntax-highlighter'
import { dracula, vs } from 'react-syntax-highlighter/dist/esm/styles/prism'
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
                            PreTag="div"
                            // showLineNumbers={true}
                            style={props.theme === 'dark' ? dracula : vs}
                            {...props_}
                        >
                            {String(children).replace(/\n$/, '')}
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
