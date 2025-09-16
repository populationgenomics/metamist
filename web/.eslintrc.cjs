module.exports = {
    env: {
        browser: true,
        es2021: true,
    },
    extends: [
        'plugin:react-hooks/recommended',
        'eslint:recommended',
        'plugin:react/recommended',
        'plugin:@typescript-eslint/recommended',
    ],
    parser: '@typescript-eslint/parser',
    parserOptions: {
        ecmaFeatures: {
            jsx: true,
        },
        ecmaVersion: 12,
        sourceType: 'module',
        project: 'tsconfig.json',
        tsconfigRootDir: __dirname,
    },
    plugins: ['react', '@typescript-eslint', 'prettier'],
    ignorePatterns: ['__generated__'],
    rules: {
        'react/jsx-filename-extension': [2, { extensions: ['.js', '.jsx', '.ts', '.tsx'] }],
        // React hasn't needed to be in scope for jsx since react 17
        'react/react-in-jsx-scope': 'off',
        'react/jsx-uses-react': 'off',
        '@typescript-eslint/naming-convention': [
            'error',
            {
                selector: ['function'],
                format: ['strictCamelCase', 'StrictPascalCase'],
                leadingUnderscore: 'allow',
            },
        ],
        'no-underscore-dangle': 'off',
        '@typescript-eslint/no-unused-vars': [
            'error',
            {
                ignoreRestSiblings: true,
                argsIgnorePattern: '^_',
                destructuredArrayIgnorePattern: '^_',
                caughtErrorsIgnorePattern: '^_',
            },
        ],
        // Turn this off for now as it is used fairly often, would be good to turn back on once
        // code is cleaned up a bit though
        '@typescript-eslint/ban-ts-comment': 0,
        // This isn't needed as ts validates prop types
        'react/prop-types': [0],
        "no-console": "error",
    },
    settings: {
        react: {
            version: 'detect',
        },
    },
}
