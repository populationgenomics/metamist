-- migrate:up

<<<<<<< HEAD
INSERT INTO assay_type (id, name) VALUES ('sequencing', 'sequencing'); 
INSERT INTO sample_type (id, name) VALUES ('blood', 'blood'); 
INSERT INTO sample_type (id, name) VALUES ('saliva', 'saliva'); 
INSERT INTO sequencing_type (id, name) VALUES ('genome', 'genome'); 
INSERT INTO sequencing_type (id, name) VALUES ('exome', 'exome'); 
INSERT INTO sequencing_type (id, name) VALUES ('transcriptome', 'transcriptome'); 
INSERT INTO sequencing_type (id, name) VALUES ('mtseq', 'mtseq'); 
INSERT INTO sequencing_type (id, name) VALUES ('chip', 'chip'); 
=======
INSERT INTO assay_type (id, name) VALUES ('sequencing', 'sequencing');
INSERT INTO sample_type (id, name) VALUES ('blood', 'blood');
INSERT INTO sample_type (id, name) VALUES ('saliva', 'saliva'); 
INSERT INTO sequencing_type (id, name) VALUES ('genome', 'genome');
INSERT INTO sequencing_type (id, name) VALUES ('exome', 'exome');
INSERT INTO sequencing_type (id, name) VALUES ('transcriptome', 'transcriptome');
INSERT INTO sequencing_type (id, name) VALUES ('mtseq', 'mtseq');
INSERT INTO sequencing_type (id, name) VALUES ('chip', 'chip');
>>>>>>> postgres-migration
INSERT INTO sequencing_technology (id, name) VALUES ('short-read', 'short-read');
INSERT INTO sequencing_technology (id, name) VALUES ('long-read', 'long-read');
INSERT INTO sequencing_technology (id, name) VALUES ('single-cell-rna-seq', 'single-cell-rna-seq');
INSERT INTO sequencing_technology (id, name) VALUES ('bulk-rna-seq', 'bulk-rna-seq');
INSERT INTO sequencing_platform (id, name) VALUES ('illumina', 'illumina');
INSERT INTO sequencing_platform (id, name) VALUES ('pacbio', 'pacbio');
INSERT INTO sequencing_platform (id, name) VALUES ('oxford-nanopore', 'oxford-nanopore');
INSERT INTO analysis_type (id, name) VALUES ('qc', 'qc');
INSERT INTO analysis_type (id, name) VALUES ('joint-calling', 'joint-calling');
INSERT INTO analysis_type (id, name) VALUES ('gvcf', 'gvcf');
INSERT INTO analysis_type (id, name) VALUES ('cram', 'cram');
INSERT INTO analysis_type (id, name) VALUES ('custom', 'custom');
INSERT INTO analysis_type (id, name) VALUES ('es-index', 'es-index');
<<<<<<< HEAD
INSERT INTO analysis_type (id, name) VALUES ('sv', 'sv'); 
=======
INSERT INTO analysis_type (id, name) VALUES ('sv', 'sv');
>>>>>>> postgres-migration
INSERT INTO analysis_type (id, name) VALUES ('web', 'web');
INSERT INTO analysis_type (id, name) VALUES ('analysis-runner', 'analysis-runner');

-- migrate:down

DELETE FROM assay_type WHERE id = 'sequencing';
DELETE FROM sample_type WHERE id IN ('blood', 'saliva');
DELETE FROM sequencing_type WHERE id IN ('genome', 'exome', 'transcriptome', 'mtseq', 'chip');
DELETE FROM sequencing_technology WHERE id IN ('short-read', 'long-read', 'single-cell-rna-seq', 'bulk-rna-seq');
DELETE FROM sequencing_platform WHERE id IN ('illumina', 'pacbio', 'oxford-nanopore');
DELETE FROM analysis_type WHERE id IN ('qc', 'joint-calling', 'gvcf', 'cram', 'custom', 'es-index', 'sv', 'web', 'analysis-runner');
