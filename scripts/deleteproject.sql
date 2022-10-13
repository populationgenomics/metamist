DECLARE @project VARCHAR(30);

SELECT @project := id FROM project WHERE name = '<dataset>';

DELETE FROM participant_phenotypes where participant_id IN (SELECT id FROM participant WHERE project = @project);
DELETE FROM family_participant WHERE family_id IN (SELECT id FROM family where project = @project);
DELETE FROM family WHERE project = @project;
DELETE FROM sample_sequencing WHERE sample_id in (SELECT id FROM sample WHERE project = @project);
DELETE FROM analysis_sample WHERE sample_id in (SELECT id FROM sample WHERE project = @project);
DELETE FROM analysis_sample WHERE analysis_id in (SELECT id FROM analysis WHERE project = @project);
DELETE FROM sample WHERE project = @project;
DELETE FROM participant WHERE project = @project;
DELETE FROM analysis WHERE project = @project;
DELETE FROM project WHERE id = @project;
