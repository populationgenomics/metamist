module.exports = {
    env: {
        browser: true,
        es2021: true
    },
    extends: [
        'airbnb-base',
        'airbnb-typescript',
        'prettier',
        'plugin:react-hooks/recommended',
        'plugin:prettier/recommended',
        'eslint:recommended',
        'plugin:react/recommended',
        'plugin:@typescript-eslint/recommended'
    ],
    parser: '@typescript-eslint/parser',
    parserOptions: {
        ecmaFeatures: {
            jsx: true
        },
        ecmaVersion: 12,
        sourceType: 'module',
        project: 'tsconfig.json',
        tsconfigRootDir: __dirname
    },
    plugins: ['react', '@typescript-eslint', 'prettier'],
    rules: {
        'react/jsx-filename-extension': [
            2,
            { extensions: ['.js', '.jsx', '.ts', '.tsx'] }
        ],
        '@typescript-eslint/no-explicit-any': 'off',
        '@typescript-eslint/naming-convention': [
            'error',
            {
                selector: ['function'],
                format: ['strictCamelCase'],
                leadingUnderscore: 'allow'
            }
        ],
        'no-underscore-dangle': 'off'
    },
    settings: {
        react: {
            version: 'detect'
        }
    }
}
