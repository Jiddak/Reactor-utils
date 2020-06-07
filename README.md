# Reactor-utils
Extra reactor utils that aren't included in either core or addons

## ReactorUtils.intersect

Intersects a flux and one other publisher. All distinct identical elements are emitted.

```Java
import com.jidda.reactorUtils;

   Flux<String> f1 = Flux.just("A","B","C");
   Flux<String> f2 = Flux.just("D","C","A");
   ReactorUtils.intersect(f1,f2).subscribe() //Emits C,A
   
   //Can also be used with prefetch value, default is Unbounded
   ReactorUtils.intersect(f1,f2,32).subscribe() //Emits C,A
   
```

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.
