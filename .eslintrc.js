module.exports = {
    "extends": "airbnb-base",
    "rules": {
        "no-console": "off",
        "linebreak-style": [
          "error",
          "unix"
        ],
        "semi": [
          "error",
          "never"
        ],
        "no-case-declarations": 0,
        "object-curly-newline": ["error", {
          "ObjectExpression": { "multiline": true },
          "ObjectPattern": { "multiline": true }
        }],
        "no-mixed-operators": [
          "error",
          {
            "groups": [
              ["+", "-", "*", "/", "%", "**"],
              ["&", "|", "^", "~", "<<", ">>", ">>>"],
              ["==", "!=", "===", "!==", ">", ">=", "<", "<="],
              ["&&", "||"],
              ["in", "instanceof"]
            ],
            "allowSamePrecedence": true
          }
        ],
        "no-restricted-syntax": "off",
        "no-underscore-dangle": "off",
        "no-plusplus": ["error", { "allowForLoopAfterthoughts": true }],
        "import/no-extraneous-dependencies": ["error", {"devDependencies": ['**/*.test.js', '**/*.spec.js', '**/setup-jest.js']}],
        "import/newline-after-import": ["error", { "count": 2 }],
        "no-shadow": "off",
        "max-len": ["error", {"code": 100, "tabWidth": 2, "ignoreStrings": false, "ignoreTemplateLiterals": false,}],
        "no-param-reassign": ["error", { "props": false }],
        'radix': ["error", "as-needed"],
        "import/prefer-default-export": "off",
        "import/no-named-as-default": "off",
        "import/no-named-as-default-member": "off",
        "camelcase": "off",
        "prefer-destructuring": ["error", {
          "VariableDeclarator": {
            "array": false,
            "object": true
          },
          "AssignmentExpression": {
            "array": false,
            "object": false
          }
        }, {
          "enforceForRenamedProperties": false
        }],
        "consistent-return": "off",
        "no-multiple-empty-lines": ["error", { "max": 2, "maxEOF": 1 }],
        "no-promise-executor-return": "off",
        "no-constructor-return": "off",
        "no-else-return": "off",
        "implicit-arrow-linebreak": "off",
        "arrow-parens": "off",
        "comma-style": ["error", "last"],
        "operator-linebreak": "off",
        "function-paren-newline": "off",
        "max-classes-per-file": "off",
    }
};
