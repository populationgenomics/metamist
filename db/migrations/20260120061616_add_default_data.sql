-- migrate:up
SET search_path TO main;

INSERT INTO "group" (name) VALUES ('project-creators') ON CONFLICT DO NOTHING;
INSERT INTO "group" (name) VALUES ('members-admin') ON CONFLICT DO NOTHING;
INSERT INTO assay_type (id, name) VALUES ('sequencing', 'sequencing') ON CONFLICT (id) DO NOTHING;
INSERT INTO sample_type (id, name) VALUES ('blood', 'blood') ON CONFLICT (id) DO NOTHING;
INSERT INTO sample_type (id, name) VALUES ('saliva', 'saliva') ON CONFLICT (id) DO NOTHING;
INSERT INTO sequencing_type (id, name) VALUES ('genome', 'genome') ON CONFLICT (id) DO NOTHING;
INSERT INTO sequencing_type (id, name) VALUES ('exome', 'exome') ON CONFLICT (id) DO NOTHING;
INSERT INTO sequencing_type (id, name) VALUES ('transcriptome', 'transcriptome') ON CONFLICT (id) DO NOTHING;
INSERT INTO sequencing_type (id, name) VALUES ('mtseq', 'mtseq') ON CONFLICT (id) DO NOTHING;
INSERT INTO sequencing_type (id, name) VALUES ('chip', 'chip') ON CONFLICT (id) DO NOTHING;
INSERT INTO sequencing_technology (id, name) VALUES ('short-read', 'short-read') ON CONFLICT (id) DO NOTHING;
INSERT INTO sequencing_technology (id, name) VALUES ('long-read', 'long-read') ON CONFLICT (id) DO NOTHING;
INSERT INTO sequencing_technology (id, name) VALUES ('single-cell-rna-seq', 'single-cell-rna-seq') ON CONFLICT (id) DO NOTHING;
INSERT INTO sequencing_technology (id, name) VALUES ('bulk-rna-seq', 'bulk-rna-seq') ON CONFLICT (id) DO NOTHING;
INSERT INTO sequencing_platform (id, name) VALUES ('illumina', 'illumina') ON CONFLICT (id) DO NOTHING;
INSERT INTO sequencing_platform (id, name) VALUES ('pacbio', 'pacbio') ON CONFLICT (id) DO NOTHING;
INSERT INTO sequencing_platform (id, name) VALUES ('oxford-nanopore', 'oxford-nanopore') ON CONFLICT (id) DO NOTHING;
INSERT INTO analysis_type (id, name) VALUES ('qc', 'qc') ON CONFLICT (id) DO NOTHING;
INSERT INTO analysis_type (id, name) VALUES ('joint-calling', 'joint-calling') ON CONFLICT (id) DO NOTHING;
INSERT INTO analysis_type (id, name) VALUES ('gvcf', 'gvcf') ON CONFLICT (id) DO NOTHING;
INSERT INTO analysis_type (id, name) VALUES ('cram', 'cram') ON CONFLICT (id) DO NOTHING;
INSERT INTO analysis_type (id, name) VALUES ('custom', 'custom') ON CONFLICT (id) DO NOTHING;
INSERT INTO analysis_type (id, name) VALUES ('es-index', 'es-index') ON CONFLICT (id) DO NOTHING;
INSERT INTO analysis_type (id, name) VALUES ('sv', 'sv') ON CONFLICT (id) DO NOTHING;
INSERT INTO analysis_type (id, name) VALUES ('web', 'web') ON CONFLICT (id) DO NOTHING;
INSERT INTO analysis_type (id, name) VALUES ('analysis-runner', 'analysis-runner') ON CONFLICT (id) DO NOTHING;

-- migrate:down

SET search_path TO main;


DELETE FROM "group" WHERE name IN ('project-creators', 'members-admin');
DELETE FROM assay_type WHERE id = 'sequencing';
DELETE FROM sample_type WHERE id IN ('blood', 'saliva');
DELETE FROM sequencing_type WHERE id IN ('genome', 'exome', 'transcriptome', 'mtseq', 'chip');
DELETE FROM sequencing_technology WHERE id IN ('short-read', 'long-read', 'single-cell-rna-seq', 'bulk-rna-seq');
DELETE FROM sequencing_platform WHERE id IN ('illumina', 'pacbio', 'oxford-nanopore');
DELETE FROM analysis_type WHERE id IN ('qc', 'joint-calling', 'gvcf', 'cram', 'custom', 'es-index', 'sv', 'web', 'analysis-runner');
