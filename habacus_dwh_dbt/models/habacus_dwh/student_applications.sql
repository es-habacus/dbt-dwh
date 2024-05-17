SELECT
    id
    , uuid
    , origin
    , created_at
    , student_id
    , certification_id
    , school_course_id
    , schoold_id
FROM {{ source('staging', 'student_applications') }}