const checklistItems = [
    'Does not contain the names of any sensitive CPG projects',
    'Version number is bumped appropriately if API is changed',
    'Tests cover any newly added code'
]
const commentTitle = '## PR Checklist ✅'

const itemRegex = /- \[(x| )\] ([\w ]+)/g

/**
 * Builds a new checklist as an array of strings based on an existing comment and an
 * array of checklist items. Any items that are already represented in the body
 * will maintain their checked status, and any new items will be added.
 * 
 * @param {string[]} items 
 * @param {string} checklistStr
 * @returns {string[]}
 */
const coverItemsFromComment = (items, checklistStr) => {
    const matches = [...checklistStr.matchAll(itemRegex)].reduce((listContents, [_, checked, itemString]) => {
        listContents.set(itemString, checked)
        return listContents
    }, new Map())
    console.log(matches)

    const newListBody = items.map((item) => {
        const checked = matches.get(item) || ' '
        return `- [${checked}] ${item}`
    })

    return newListBody
}

/**
 * Constructs a new comment from title and checklist body strings.
 * @param {string} title 
 * @param {string} checklist 
 * @returns {string}
 */
const buildCommentFromChecklist = (title, checklist) => {
    return `
${title}
Have you checked the following?
${checklist}

**Check all boxes before merging.**
`
}

module.exports = async ({ github, context, core }) => {
    try {
        const { owner, repo } = context.repo
        const prNumber = context.issue.number

        // Look for existing checklist comment.
        const comments = await github.rest.issues.listComments({
            owner,
            repo,
            issue_number: prNumber
        })

        const botComment = comments.data.find(comment => 
            comment.body.includes(commentTitle) && 
            comment.user.login === 'github-actions[bot]'
        ) || ''
        
        const checklistBody = coverItemsFromComment(checklistItems, botComment).join('\n')
        const checklist = buildCommentFromChecklist(checklistTitle, checklistBody)

        if (botComment) {
        // Update existing comment.
        await github.rest.issues.updateComment({
            owner,
            repo,
            comment_id: botComment.id,
            body: checklist
        });
        } else {
        // Create new comment.
        await github.rest.issues.createComment({
            owner,
            repo,
            issue_number: prNumber,
            body: checklist
        });
        }
    } catch (error) {
        core.setFailed(`Failed to manage checklist comment: ${error.message}`);
    }
}