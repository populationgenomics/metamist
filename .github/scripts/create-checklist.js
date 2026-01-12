const checklist = `
## PR Checklist ✅
Have you checked the following?
- [ ] Does not contain the names of any sensitive CPG projects
- [ ] Version number is bumped appropriately if API is changed
- [ ] Tests cover any newly added code

*Check all boxes before merging.*`;


module.exports = async ({ github, context, core }) => {
    try {
        const { owner, repo } = context.repo;
        const prNumber = context.issue.number;

        // Look for existing checklist comment.
        const comments = await github.rest.issues.listComments({
            owner,
            repo,
            issue_number: prNumber
        });

        const botComment = comments.data.find(comment => 
        comment.body.includes('## PR Checklist') && 
        comment.user.login === 'github-actions[bot]'
        );

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