pub struct QcRule {
    pub pattern: [i32; 3],
    pub code: i32,
}

const QC_RULES: [QcRule; 22] = [
    QcRule {
        pattern: [1, 9, 9],
        code: 5,
    },
    QcRule {
        pattern: [1, 9, 9],
        code: 5,
    },
    QcRule {
        pattern: [1, 9, 9],
        code: 5,
    },
    QcRule {
        pattern: [4, 9, 9],
        code: 5,
    },
    QcRule {
        pattern: [5, 9, 9],
        code: 5,
    },
    QcRule {
        pattern: [8, 9, 9],
        code: 5,
    },
    QcRule {
        pattern: [8, 9, 8],
        code: 7,
    },
    QcRule {
        pattern: [3, 3, 9],
        code: 7,
    },
    QcRule {
        pattern: [3, 0, 8],
        code: 7,
    },
    QcRule {
        pattern: [0, 0, 8],
        code: 7,
    },
    QcRule {
        pattern: [-1, -1, 1],
        code: 1,
    },
    QcRule {
        pattern: [-1, -1, 2],
        code: 1,
    },
    QcRule {
        pattern: [-1, -1, 3],
        code: 6,
    },
    QcRule {
        pattern: [-1, -1, 4],
        code: 6,
    },
    QcRule {
        pattern: [-1, -1, 5],
        code: 1,
    },
    QcRule {
        pattern: [-1, -1, 6],
        code: 1,
    },
    QcRule {
        pattern: [-1, 0, -1],
        code: 0,
    },
    QcRule {
        pattern: [-1, 9, -1],
        code: 2,
    },
    QcRule {
        pattern: [-1, 1, 0],
        code: 4,
    },
    QcRule {
        pattern: [-1, 2, 0],
        code: 5,
    },
    QcRule {
        pattern: [-1, 3, 0],
        code: 7,
    },
    QcRule {
        pattern: [-1, 3, 8],
        code: 7,
    },
];

// Extracts a subset of the `useinfo` flag used to match against the QC rule patterns.
// `useinfo` is expected to be at least 5 numeric char long
pub fn extract_flag(useinfo: &str) -> [i32; 3] {
    let mut iter = useinfo.chars().skip(1).take(3).map(|x| x.to_digit(10));
    std::array::from_fn(|_| iter.next().unwrap().unwrap() as i32)
}

// Checks if the flag extracted from the given useinfo matches
// any of the QC rules, and returns the corresponding quality code
pub fn get_quality_code(useinfo: &str) -> Option<i32> {
    let flag = extract_flag(useinfo);

    QC_RULES
        .iter()
        .find(|rule| {
            rule.pattern
                .iter()
                .enumerate()
                .all(|(i, p)| *p < 0 || flag[i] == *p)
        })
        .map(|rule| rule.code)
}
