# histodu: Summarize distribution of file sizes.

Want to know how many "small" files do you have? What's the typical file size?
Take a quick look at the size distribution of your filesystems.

```console
$ histodu /nix/store
count = 14299237
mean = 46.1 kiB
0% = 1 B
50% = 815 B
90% = 17.1 KB
99% = 330.8 KB
100% = 1.2 GB
72.925% = 4.0 kiB
96.197% = 64.0 kiB
99.584% = 1.0 MiB

11.25user 154.21system 0:12.16elapsed 1360%CPU (0avgtext+0avgdata 507416maxresident)k
2087168inputs+0outputs (0major+145077minor)pagefaults 0swaps
```

## License

MIT OR Apache-2.0.

## Credit

- Motivated by <https://farseerfc.me/file-size-histogram.html>.
- This project is named by @dramforever at github.
