# Reactor-utils
Extra reactor utils that aren't included in either core or addons

## ReactorUtils#intersect

Intersects multiple publishers. All distinct identical elements are emitted.

```Java
import com.jidda.reactorUtils;

	Flux<String> f1 = Flux.just("A","B","C");
	Flux<String> f2 = Flux.just("D","C","A");
	Flux<String> f3 = Flux.just("F","B","D");
	ReactorUtils.intersect(f1,f2).subscribe() //Emits C,A
   
	//Can also be used with prefetch value, default is Unbounded
	ReactorUtils.intersect(f1,f2,32).subscribe() //Emits C,A

	//Can also be used with list of publishers
	ReactorUtils.intersect(Arrays.asList(f1,f2,f3)).subscribe() //Emits C,A,B,D
```

---

## ReactorUtils#joinIf

Joins two publishers values, emits based upon filter condition.

```Java
import com.jidda.reactorUtils;

    Flux<String> f1 = Flux.just("A","B","C");
    Flux<Integer> f2 = Flux.just(1,5,2);
    final String alphabet = "ABC";

    ReactorUtils.joinIf(f1,
            f2,
            (a,b) -> a,
            (a,b) -> b.equals(alphabet.indexOf(a)+1)
    ).subscribe() // Emits A,B
)
```

**Important:**
Unlike Flux#join, the leftEnd and rightEnd functions have not yet been implemented so the two joined fluxes must terminate on their own

---

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.
