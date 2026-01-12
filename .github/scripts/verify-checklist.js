const commentTitle = '## PR Checklist ✅'

module.exports = async ({ github, context, core }) => {
  try {
    const { data: comments } = await github.rest.issues.listComments({
      owner: context.repo.owner,
      repo: context.repo.repo,
      issue_number: context.issue.number,
    });

    const checklistComment = comments.find(
      (comment) =>
        comment.user.login === 'github-actions[bot]' && comment.body.includes(commentTitle)
    );

    if (!checklistComment) {
      core.setFailed('Checklist comment not found');
      return;
    }

    const uncheckedItems = checklistComment.body
      .split('\n')
      .filter((line) => line.trim().startsWith('- [ ]'));

    if (uncheckedItems.length > 0) {
      core.setFailed(`Unfinished checklist items: ${uncheckedItems.length}`);
    } else {
      core.info('All checklist items completed');
    }
  } catch (error) {
    core.setFailed(`Checklist verification failed: ${error.message}`);
  }
};