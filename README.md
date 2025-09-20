# SmartFLow

SmartFLow is an SDN-integrated framework designed to enhance communication efficiency in cross-silo Federated Learning (FL) environments. By leveraging Software-Defined Networking (SDN) with the ONOS controller, SmartFLow dynamically optimizes routing paths to reduce latency, mitigate network congestion, and improve synchronization in FL deployments. The framework achieves up to 47% reduction in training time compared to traditional routing methods, as demonstrated in experiments across diverse network topologies.

## Features
- **Dynamic Path Optimization**: Implements two path selection strategies—Constraint Optimization (SmartFLow_CP) and Greedy (SmartFLow_Greedy)—to minimize maximum completion time in synchronous FL.
- **Real-Time Network Monitoring**: Uses OpenFlow statistics to track link latency, packet loss, and throughput, enabling adaptive routing decisions.
- **Flexible Testbed**: Supports customizable network topologies via TopoHub, emulates congestion with iperf3, and monitors FL training metrics.
- **Open-Source Implementation**: Built with ONOS, Flower AI, PyTorch, and Google’s CP-SAT solver (OR-Tools Java API v9.12).

## Key results:
- **SmartFLow_CP**: Achieves up to 43% reduction in training time for E1 and 45% for E3 compared to shortest-path routing (RFWD).
- **SmartFLow_Greedy**: Reduces training time by 47% in E2, with lower computational overhead (10–40 ms vs. 35–200 ms for CP).
- Metrics include time to 60% and 80% accuracy, average round time, path reassignments, and gRPC timeouts.

For detailed results, see the paper: *SmartFLow: A Communication-Efficient SDN Framework for Cross-Silo Federated Learning*.

## Contributing
Contributions are welcome! Please follow these steps:
1. Fork the repository.
2. Create a feature branch (`git checkout -b feature/your-feature`).
3. Commit changes (`git commit -m "Add your feature"`).
4. Push to the branch (`git push origin feature/your-feature`).
5. Open a pull request.

Please ensure code adheres to PEP 8 standards and includes unit tests.

## License
This work is licensed under the [Creative Commons Attribution 4.0 International License (CC BY 4.0)](https://creativecommons.org/licenses/by/4.0/).
You are free to share and adapt the material for any purpose, even commercially, provided appropriate credit is given.
## Citation
If you use SmartFLow in your research, please cite:
```bibtex
@misc{hamdan2025smartflowcommunicationefficientsdnframework,
      title={SmartFLow: A Communication-Efficient SDN Framework for Cross-Silo Federated Learning}, 
      author={Osama Abu Hamdan and Hao Che and Engin Arslan and Md Arifuzzaman},
      year={2025},
      eprint={2509.00603},
      archivePrefix={arXiv},
      primaryClass={cs.NI},
      url={https://arxiv.org/abs/2509.00603}, 
}
```

## Contact
For questions or issues, please open an issue on GitHub
