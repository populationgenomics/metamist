-- migrate:up

SET search_path TO main;

-- RFC 7396 JSON Merge Patch implementation
-- https://datatracker.ietf.org/doc/html/rfc7396
--
-- The algorithm is defined in Section 2 of the RFC:
--
-- define MergePatch(Target, Patch):
--   if Patch is an Object:
--     if Target is not an Object:
--       Target = {} # Ignore the contents and set it to an empty Object
--     for each Name/Value pair in Patch:
--       if Value is null:
--         if Name exists in Target:
--           remove the Name/Value pair from Target
--       else:
--         Target[Name] = MergePatch(Target[Name], Value)
--     return Target
--   else:
--     return Patch


CREATE OR REPLACE FUNCTION json_merge_patch(target JSONB, patch JSONB)
RETURNS JSONB
LANGUAGE plpgsql
IMMUTABLE
STRICT
PARALLEL SAFE
AS $$
DECLARE
    result JSONB;
    key TEXT;
    patch_value JSONB;
    target_value JSONB;
BEGIN
    -- If patch is not an object, return the patch (replaces target entirely)
    IF jsonb_typeof(patch) != 'object' THEN
        RETURN patch;
    END IF;

    -- If patch is an object but target is not, start with empty object
    IF jsonb_typeof(target) != 'object' THEN
        target := '{}';
    END IF;

    -- Start with the target
    result := target;

    -- Iterate over each key in the patch
    FOR key IN SELECT jsonb_object_keys(patch)
    LOOP
        patch_value := patch -> key;

        -- If patch value is null, remove the key from result
        IF patch_value IS NULL OR jsonb_typeof(patch_value) = 'null' THEN
            result := result - key;
        ELSE
            -- Get the current target value for this key (may be null if key doesn't exist)
            target_value := result -> key;

            -- If target doesn't have this key, use null as the base
            IF target_value IS NULL THEN
                target_value := 'null'::jsonb;
            END IF;

            -- Recursively merge and set the result
            result := jsonb_set(result, ARRAY[key], json_merge_patch(target_value, patch_value));
        END IF;
    END LOOP;

    RETURN result;
END;
$$;


-- migrate:down

SET search_path TO main;

DROP FUNCTION IF EXISTS json_merge_patch(JSONB, JSONB);
