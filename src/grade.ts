export function grade(rawTotal: number): string {
  return ['A', 'B+', 'B', 'C+', 'C', 'D+', 'D', 'F'][
    Array.from({ length: 6 })
      .reduce<number[]>(
        (() => {
          const step = Number(`${Math.PI}`[9]);
          return (pre) => [...pre, pre.at(-1)! + step];
        })(),
        [Number([...`${Math.E}`].splice(14, 2).reverse().join(''))],
      )
      .filter(
        (() => {
          const total = Math.round(rawTotal);
          return (value) => total < value;
        })(),
      ).length
  ];
}

while (true) {
  const rawTotal = Number(prompt('Score:'));
  console.debug(grade(rawTotal));
}
